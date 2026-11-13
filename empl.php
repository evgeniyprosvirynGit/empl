<?php

declare(strict_types=1);

namespace App\Shop\Services\Checkout;

use App\Classes\Database\DatabaseExceptionInspector;
use App\Classes\Database\DatabaseTransaction;
use App\Enums\CheckoutAttemptStatus;
use App\Events\Checkout\CheckoutOrderCreated;
use App\Models\Checkout\CheckoutAttempt;
use App\Models\Order\Order;
use App\Shop\Exceptions\CheckoutAttemptOrderMismatchException;
use App\Shop\Exceptions\InvalidCheckoutAttemptTransitionException;
use App\Shop\Repositories\Checkout\CheckoutAttemptRepository;
use App\Shop\Repositories\Order\OrderRepository;
use Illuminate\Database\QueryException;
use Throwable;

final readonly class CheckoutAttemptService
{
    public function __construct(
        private CheckoutAttemptRepository $checkoutAttemptRepository,
        private OrderRepository $orderRepository,
        private DatabaseTransaction $transaction,
        private DatabaseExceptionInspector $databaseExceptions,
    ) {}

    public function queue(
        int $cartId,
        ?int $userId,
        string $sessionId,
        string $idempotencyKey,
    ): CheckoutAttempt {
        try {
            return $this->transaction->run(function () use ($cartId, $userId, $sessionId, $idempotencyKey): CheckoutAttempt {
                $existingAttempt = $this->checkoutAttemptRepository->findByIdempotencyKeyForIdentity(
                    userId: $userId,
                    sessionId: $sessionId,
                    idempotencyKey: $idempotencyKey,
                    lockForUpdate: true,
                );

                if ($existingAttempt !== null) {
                    return $existingAttempt;
                }

                return $this->checkoutAttemptRepository->createQueued(
                    cartId: $cartId,
                    userId: $userId,
                    sessionId: $sessionId,
                    idempotencyKey: $idempotencyKey,
                );
            });
        } catch (QueryException $exception) {
            if (! $this->databaseExceptions->isUniqueConstraintViolation($exception)) {
                throw $exception;
            }

            return $this->checkoutAttemptRepository->findByIdempotencyKeyForIdentityOrFail(
                userId: $userId,
                sessionId: $sessionId,
                idempotencyKey: $idempotencyKey,
            );
        }
    }

    public function begin(int $checkoutAttemptId): CheckoutAttempt
    {
        return $this->transaction->run(function () use ($checkoutAttemptId): CheckoutAttempt {
            $attempt = $this->checkoutAttemptRepository->findForUpdateOrFail($checkoutAttemptId);

            if ($attempt->status === CheckoutAttemptStatus::COMPLETED) {
                return $attempt;
            }

            $this->assertTransition($attempt, CheckoutAttemptStatus::PROCESSING);

            $this->checkoutAttemptRepository->update($attempt, [
                'status' => CheckoutAttemptStatus::PROCESSING,
                'processing_attempts' => $attempt->processing_attempts + 1,
                'started_at' => now(),
                'last_error' => null,
                'failed_at' => null,
            ]);

            return $attempt;
        });
    }

    public function publishOrderCreated(int $checkoutAttemptId, Order $order): void
    {
        $this->transaction->run(function () use ($checkoutAttemptId, $order): void {
            if ($this->checkoutAttemptRepository->markOrderCreated(
                $checkoutAttemptId,
                (int) $order->getKey(),
            )) {
                CheckoutOrderCreated::dispatch(
                    checkoutAttemptId: $checkoutAttemptId,
                    orderId: (int) $order->getKey(),
                );

                return;
            }

            $attempt = $this->checkoutAttemptRepository->findForUpdateOrFail($checkoutAttemptId);

            if ($attempt->status === CheckoutAttemptStatus::COMPLETED) {
                return;
            }

            if ($attempt->order_id !== null && (int) $attempt->order_id !== (int) $order->getKey()) {
                throw new CheckoutAttemptOrderMismatchException(
                    attemptOrderId: (int) $attempt->order_id,
                    providedOrderId: (int) $order->getKey(),
                );
            }

            $this->assertTransition($attempt, CheckoutAttemptStatus::ORDER_CREATED);

            $this->checkoutAttemptRepository->update($attempt, [
                'order_id' => $order->getKey(),
                'status' => CheckoutAttemptStatus::ORDER_CREATED,
                'order_created_at' => $attempt->order_created_at ?? now(),
                'last_error' => null,
                'failed_at' => null,
            ]);

            CheckoutOrderCreated::dispatch(
                checkoutAttemptId: (int) $attempt->getKey(),
                orderId: (int) $order->getKey(),
            );
        });
    }

    public function markFailed(int $checkoutAttemptId, ?Throwable $exception): void
    {
        $this->transaction->run(function () use ($checkoutAttemptId, $exception): void {
            $attempt = $this->checkoutAttemptRepository->findForUpdateOrFail($checkoutAttemptId);

            if ($attempt->status === CheckoutAttemptStatus::COMPLETED) {
                return;
            }

            $orderId = $attempt->order_id;

            if ($orderId === null) {
                $orderId = $this->orderRepository
                    ->findByIdempotencyKeyForIdentity(
                        $attempt->idempotency_key,
                        $attempt->user_id,
                        (string) $attempt->session_id,
                    )
                    ?->getKey();
            }

            $status = $orderId !== null
                ? CheckoutAttemptStatus::NOTIFICATION_FAILED
                : CheckoutAttemptStatus::FAILED;

            $this->assertTransition($attempt, $status);

            $this->checkoutAttemptRepository->update($attempt, [
                'order_id' => $orderId,
                'status' => $status,
                'last_error' => $this->errorMessage($exception),
                'failed_at' => now(),
            ]);
        });
    }

    public function markNotificationsPendingForDelivery(CheckoutAttempt $attempt): void
    {
        $this->transitionAttempt($attempt, CheckoutAttemptStatus::NOTIFICATIONS_PENDING);
    }

    public function recoverFailedNotificationDelivery(CheckoutAttempt $attempt): void
    {
        if ($attempt->status !== CheckoutAttemptStatus::NOTIFICATION_FAILED) {
            return;
        }

        $this->markNotificationsPendingForDelivery($attempt);
    }

    public function markNotificationsCompleted(CheckoutAttempt $attempt): void
    {
        $this->transitionAttempt(
            attempt: $attempt,
            targetStatus: CheckoutAttemptStatus::COMPLETED,
            attributes: ['completed_at' => now()],
        );
    }

    public function markNotificationDeliveryFailed(CheckoutAttempt $attempt, string $errorMessage): void
    {
        if (
            $attempt->status === CheckoutAttemptStatus::COMPLETED
            || ! $attempt->status->canTransitionTo(CheckoutAttemptStatus::NOTIFICATION_FAILED)
        ) {
            return;
        }

        $this->checkoutAttemptRepository->update($attempt, [
            'status' => CheckoutAttemptStatus::NOTIFICATION_FAILED,
            'last_error' => $errorMessage,
            'failed_at' => now(),
        ]);
    }

    private function transitionAttempt(
        CheckoutAttempt $attempt,
        CheckoutAttemptStatus $targetStatus,
        array $attributes = [],
    ): void {
        $this->assertTransition($attempt, $targetStatus);

        $this->checkoutAttemptRepository->update($attempt, [
            ...$attributes,
            'status' => $targetStatus,
            'last_error' => null,
            'failed_at' => null,
        ]);
    }

    private function assertTransition(CheckoutAttempt $attempt, CheckoutAttemptStatus $status): void
    {
        if ($attempt->status->canTransitionTo($status)) {
            return;
        }

        throw new InvalidCheckoutAttemptTransitionException($attempt->status, $status);
    }

    private function errorMessage(?Throwable $exception): ?string
    {
        if ($exception === null) {
            return null;
        }

        return mb_substr($exception->getMessage(), 0, 5000);
    }
}
