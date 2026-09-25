package com.shopmart.notification.event;

/**
 * Các loại sự kiện trong luồng Saga đặt hàng (bản sao dùng cho notification-service).
 */
public enum SagaEventType {
    ORDER_CREATED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED,
    INVENTORY_RELEASED
}
