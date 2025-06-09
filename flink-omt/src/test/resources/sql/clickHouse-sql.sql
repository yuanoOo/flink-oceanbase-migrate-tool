CREATE TABLE IF NOT EXISTS test1.orders1 (
                                             order_id      Int32,
                                             order_date    DateTime,
                                             customer_name String,
                                             price         Float64,
                                             product_id    Int32,
                                             order_status  Int32 DEFAULT 0
)
ENGINE = MergeTree()
PRIMARY KEY (order_id)
ORDER BY (order_id);

CREATE TABLE IF NOT EXISTS test1.orders2 (
                                            order_id UInt64,
                                            customer_name String,
                                            product_code String,
                                            product_name String,
                                            price Decimal(18, 2),
    quantity UInt32,
    total_amount Decimal(18, 2),
    order_time DateTime
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(order_time)
    ORDER BY (order_time, order_id);

INSERT INTO test1.orders1 (order_id, order_date, customer_name, price, product_id)
VALUES
    (1, '2025-06-01 10:00:00', 'Alice', 199.99, 101),
    (2, '2025-06-01 10:05:00', 'Bob', 299.99, 102);


INSERT INTO test1.orders2 (
    order_id, customer_name, product_code, product_name, price, quantity, total_amount, order_time
) VALUES
      (1001, 'Alice', 'P1001', 'Laptop', 9999.99, 2, 19999.98, '2025-01-15 10:00:00'),
      (1002, 'Bob', 'P1002', 'Smartphone', 4999.99, 1, 4999.99, '2025-01-20 11:30:00'),
      (1003, 'Charlie', 'P1003', 'Tablet', 2999.99, 3, 8999.97, '2025-02-05 09:15:00'),
      (1004, 'David', 'P1004', 'Monitor', 1999.99, 2, 3999.98, '2025-02-10 14:45:00'),
      (1005, 'Eva', 'P1005', 'Keyboard', 199.99, 5, 999.95, '2025-03-01 08:30:00');