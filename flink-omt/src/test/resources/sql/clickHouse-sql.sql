/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
-- test1.orders1 definition
CREATE TABLE test1.orders1
(
    `order_id` Int32,
    `order_date` DateTime,
    `customer_name` String,
    `price` Float64,
    `product_id` Int32,
    `order_status` Int32 DEFAULT 0
)ENGINE = MergeTree
PRIMARY KEY order_id
ORDER BY order_id;

-- test1.orders2 definition
CREATE TABLE test1.orders2
(
    `order_id` UInt64,
    `customer_name` String,
    `product_code` String,
    `product_name` String,
    `price` Decimal(18,2),
    `quantity` UInt32,
    `total_amount` Decimal(18,2),
    `order_time` DateTime
) ENGINE = MergeTree
PARTITION BY toYYYYMM(order_time)
ORDER BY (order_time,order_id);


-- test1.orders3 definition
CREATE TABLE test1.orders3
(
    `UserID` UInt64,
    `SessionID` UUID
) ENGINE = MergeTree
PARTITION BY sipHash64(UserID) % 16
ORDER BY UserID;

-- test1.orders4 definition
CREATE TABLE test1.orders4
(
    `order_id` UInt64,
    `region` LowCardinality(String),
    `order_date` Date,
    `amount` Decimal(10,2)
)ENGINE = MergeTree
PARTITION BY (region,toYYYYMM(order_date))
ORDER BY order_date;

-- test1.test definition
CREATE TABLE test1.orders5
(
    `order_id` UInt64,
    `region` LowCardinality(String),
    `order_date` Date,
    `amount` Decimal(10,2)
) ENGINE = MergeTree
PARTITION BY region
ORDER BY order_date;


INSERT INTO test1.orders1
(order_id, order_date, customer_name, price, product_id, order_status)
VALUES(1, '2025-06-01 10:00:00', 'Alice', 199.99, 101, 0);
INSERT INTO test1.orders1
(order_id, order_date, customer_name, price, product_id, order_status)
VALUES(2, '2025-06-01 10:05:00', 'Bob', 299.99, 102, 0);


INSERT INTO test1.orders2
(order_id, customer_name, product_code, product_name, price, quantity, total_amount, order_time)
VALUES(1001, 'Alice', 'P1001', 'Laptop', 9999.99, 2, 19999.98, '2025-01-15 10:00:00');
INSERT INTO test1.orders2
(order_id, customer_name, product_code, product_name, price, quantity, total_amount, order_time)
VALUES(1002, 'Bob', 'P1002', 'Smartphone', 4999.99, 1, 4999.99, '2025-01-20 11:30:00');
INSERT INTO test1.orders2
(order_id, customer_name, product_code, product_name, price, quantity, total_amount, order_time)
VALUES(1003, 'Charlie', 'P1003', 'Tablet', 2999.99, 3, 8999.97, '2025-02-05 09:15:00');
INSERT INTO test1.orders2
(order_id, customer_name, product_code, product_name, price, quantity, total_amount, order_time)
VALUES(1004, 'David', 'P1004', 'Monitor', 1999.99, 2, 3999.98, '2025-02-10 14:45:00');
INSERT INTO test1.orders2
(order_id, customer_name, product_code, product_name, price, quantity, total_amount, order_time)
VALUES(1005, 'Eva', 'P1005', 'Keyboard', 199.99, 5, 999.95, '2025-03-01 08:30:00');


INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(4, '098aab6a-7c14-4503-87ac-347fba19afed');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(7, '5ee55001-bb7e-4532-b8ba-a6dc3fbb563b');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(10, 'bfde490c-447a-417b-ac48-554cd72f809f');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(29, 'dff06130-6e70-4b27-9545-d890c882bd71');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(40, 'fc8887a6-0034-439d-8825-1614e777afe9');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(85, '35f88bc6-984c-4970-9618-c72017f337f5');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(91, '9ade3e35-2eaa-4dcf-af94-5bbfa139594c');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(119, 'c6967923-0ac0-4bbc-ab77-d8867a77d060');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(120, '39afde3b-c75e-4ab2-9bee-e7c1a70b5514');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(134, '66fdaccc-8fe3-40e8-953f-769814907f57');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(144, '7a5e8e40-45ee-4fb9-be11-d5c0a2314581');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(159, 'e931856a-d26e-40c1-ae57-4ba1b83e320a');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(162, '2bc9b3d9-d3a8-44ec-92c4-edf1de4f068b');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(175, '14ef8b26-831e-4894-85f2-38fbd7c2ed10');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(176, 'b07aae78-a6ff-46a4-9d9c-5deedb85104d');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(179, '68f3d77a-62e1-454f-ba82-e65ce5cb170a');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(196, 'cf092243-aceb-4fa4-94fa-002cbf04c76d');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(199, '6dbf7bca-76e3-46cd-9a37-50f1faaa4b3d');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(208, '35e7918b-3095-4f23-8385-b74a26d1d05f');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(245, '7e5b9a38-0d47-40bf-8f51-7eca93df020c');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(268, '6d3dd081-c224-4405-8552-afa30ee3edf8');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(278, '74aa2683-d952-4f67-8b8c-a7de1a45a1cd');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(290, 'a86a15df-4523-45c4-b2ee-111bc9241d55');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(299, '38c78905-1fec-48b0-a765-8712e17cdcae');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(300, '905b50e2-bc92-486e-94e0-a9ac7ed3e7c5');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(334, 'c6d2553b-b7a6-4aba-94c4-3f9b8ccf4bdd');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(337, '57d4e894-4ae7-4574-b979-277a010fb7d7');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(347, '0c5f3a0f-cc6c-4d80-8ea6-3b7dbb7183fc');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(350, '99e5d166-bf1f-4b8d-b618-02afc89eeb17');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(364, '760d483f-f2b1-47c1-9b9a-c51a52b34498');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(385, '07ee795e-02d5-4789-884c-80058f4028b3');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(396, '3783160c-da1a-443c-a11f-abce3d32bb2c');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(402, '25461ce8-a265-404c-8382-8112768a48ea');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(441, 'cbd322a9-997b-49a2-9d6d-4d94f1345efe');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(452, '75bba556-7dac-414d-9a02-ef7ac74d042d');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(465, '9dbf8790-51a0-4118-9427-72a68f122e56');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(481, '8fb72f23-5f37-4796-9c1c-04f83276ca61');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(487, '224fa36c-fc41-4034-a888-f15702245829');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(500, 'd1122731-99e8-4ea1-bdf4-a3011033fd0f');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(511, 'd9454325-8e27-486b-95c2-20def8dc7900');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(518, '37bd580e-4da1-456c-a132-e57f7b084912');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(524, 'd5f0b57b-322e-4323-8b87-1086084ec032');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(530, '2aa782dd-bbcd-456d-8f49-b82e573e3db9');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(536, 'd6491b04-61b4-44cf-9d47-f221d490b2d7');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(543, '1f84fea7-6634-4f1c-b1a9-7e48bf9a0ce5');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(546, '6f02b9e4-46e2-4bb0-a744-a1a009ad1783');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(565, 'df8997ce-228d-475d-aa17-21ef2d3dbf4c');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(600, '4b77bb0b-3d63-4f59-9299-3700481e2f6e');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(603, 'c1702c4d-ea27-4184-9685-7abe03eaf981');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(609, '05ee3946-f8ad-4281-8559-d0342237aedb');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(611, '6f7b08e1-5998-4f11-ad90-1d26c9840c6c');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(627, 'a6f627c5-7460-4f5b-b121-920aa3ba1934');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(630, '12a68868-296c-4a23-b6d9-778026eccef4');
INSERT INTO test1.orders3
(UserID, SessionID)
VALUES(635, '5f693b34-8ed1-4fee-8fc4-8109d4fdc591');


INSERT INTO test1.orders4
(order_id, region, order_date, amount)
VALUES(8, 'West', '2025-06-20', 800.00);
INSERT INTO test1.orders4
(order_id, region, order_date, amount)
VALUES(3, 'East', '2025-05-15', 300.25);
INSERT INTO test1.orders4
(order_id, region, order_date, amount)
VALUES(1, 'North', '2025-06-01', 100.50);
INSERT INTO test1.orders4
(order_id, region, order_date, amount)
VALUES(5, 'North', '2025-06-03', 500.00);
INSERT INTO test1.orders4
(order_id, region, order_date, amount)
VALUES(6, 'South', '2025-05-12', 600.00);
INSERT INTO test1.orders4
(order_id, region, order_date, amount)
VALUES(4, 'West', '2025-04-10', 400.00);
INSERT INTO test1.orders4
(order_id, region, order_date, amount)
VALUES(7, 'East', '2025-06-18', 700.00);
INSERT INTO test1.orders4
(order_id, region, order_date, amount)
VALUES(2, 'South', '2025-06-02', 200.75);


INSERT INTO test1.orders5
(order_id, region, order_date, amount)
VALUES(4, 'West', '2025-06-04', 300.00);
INSERT INTO test1.orders5
(order_id, region, order_date, amount)
VALUES(1, 'North', '2025-06-01', 100.00);
INSERT INTO test1.orders5
(order_id, region, order_date, amount)
VALUES(5, 'North', '2025-06-05', 250.00);
INSERT INTO test1.orders5
(order_id, region, order_date, amount)
VALUES(2, 'South', '2025-06-02', 200.00);
INSERT INTO test1.orders5
(order_id, region, order_date, amount)
VALUES(6, 'South', '2025-06-06', 180.00);
INSERT INTO test1.orders5
(order_id, region, order_date, amount)
VALUES(3, 'East', '2025-06-03', 150.00);

