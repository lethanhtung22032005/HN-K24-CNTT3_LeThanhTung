package com.shopmart.order.client;

import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.exception.InventoryUnavailableException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Câu 2: Bọc InventoryClient bằng Resilience4j {@link CircuitBreaker} + fallback method.
 *
 * <p>Vòng đời 3 trạng thái của Circuit Breaker:</p>
 * <ul>
 *   <li><b>CLOSED</b> (đóng): cho phép gọi bình thường, thống kê tỉ lệ lỗi.</li>
 *   <li><b>OPEN</b> (mở): khi tỉ lệ lỗi vượt ngưỡng (50% trong sliding window 10),
 *       chặn mọi lời gọi tới inventory-service và trả về fallback ngay lập tức
 *       -> tránh Cascading Failure (lỗi dây chuyền).</li>
 *   <li><b>HALF-OPEN</b> (hé mở): sau wait-duration (10s), cho phép vài request thử
 *       để kiểm tra service đã hồi phục chưa; thành công thì về CLOSED, thất bại lại về OPEN.</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryGateway {

    private final InventoryClient inventoryClient;

    @CircuitBreaker(name = "inventory", fallbackMethod = "getProductFallback")
    public ProductDto getProduct(Long productId) {
        ProductDto product = inventoryClient.getProduct(productId);
        log.info("[CB CLOSED] Lấy sản phẩm id={} từ inventory-service: name={}, price={}",
                productId, product.getName(), product.getPrice());
        return product;
    }

    /** Fallback khi inventory-service lỗi/timeout hoặc mạch đang OPEN. */
    public ProductDto getProductFallback(Long productId, Throwable throwable) {
        log.warn("[CB FALLBACK] Không lấy được sản phẩm id={} ({}). Trả về dữ liệu mặc định để không sập đơn hàng.",
                productId, throwable.getMessage());
        return ProductDto.builder()
                .id(productId)
                .name("UNKNOWN (inventory-service không khả dụng)")
                .price(BigDecimal.ZERO)
                .stock(0)
                .build();
    }

    @CircuitBreaker(name = "inventory", fallbackMethod = "decreaseStockFallback")
    public ProductDto decreaseStock(Long productId, int quantity) {
        log.info("[CB CLOSED] Yêu cầu trừ tồn kho id={} quantity={}", productId, quantity);
        return inventoryClient.decreaseStock(productId, new com.shopmart.order.dto.StockRequest(quantity));
    }

    public ProductDto decreaseStockFallback(Long productId, int quantity, Throwable throwable) {
        log.error("[CB FALLBACK] Trừ tồn kho id={} quantity={} thất bại: {}", productId, quantity, throwable.getMessage());
        throw new InventoryUnavailableException(
                "inventory-service không khả dụng, không thể trừ tồn kho id=" + productId, throwable);
    }

    @CircuitBreaker(name = "inventory", fallbackMethod = "increaseStockFallback")
    public ProductDto increaseStock(Long productId, int quantity) {
        log.info("[CB CLOSED] Yêu cầu hoàn tồn kho id={} quantity={}", productId, quantity);
        return inventoryClient.increaseStock(productId, new com.shopmart.order.dto.StockRequest(quantity));
    }

    public ProductDto increaseStockFallback(Long productId, int quantity, Throwable throwable) {
        log.error("[CB FALLBACK] Hoàn tồn kho id={} quantity={} thất bại: {}", productId, quantity, throwable.getMessage());
        throw new InventoryUnavailableException(
                "inventory-service không khả dụng, không thể hoàn tồn kho id=" + productId, throwable);
    }

    @CircuitBreaker(name = "inventory", fallbackMethod = "instanceFallback")
    public String instance() {
        return inventoryClient.instance();
    }

    public String instanceFallback(Throwable throwable) {
        return "inventory-service unavailable: " + throwable.getMessage();
    }
}
