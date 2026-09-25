package com.shopmart.order.exception;

/**
 * Ném ra khi inventory-service không khả dụng và fallback của Circuit Breaker
 * không thể xử lý an toàn (ví dụ: cần trừ tồn kho nhưng service đang chết).
 */
public class InventoryUnavailableException extends RuntimeException {

    public InventoryUnavailableException(String message) {
        super(message);
    }

    public InventoryUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
