drop table if exists customers_raw;
drop table if exists sales_raw;
drop table if exists after_sales_raw;

create table customers_raw (
    id int auto_increment primary key,
    name varchar(255),
    dob date,
    created_at datetime
);
create table sales_raw (
    vin varchar(255) primary key,
    customer_id int,
    model varchar(255),
    invoice_date date,
    price varchar(20),
    created_at datetime
);
create table after_sales_raw (
    service_ticket varchar(255) primary key,
    vin varchar(255),
    customer_id int,
    model varchar(255),
    service_date date,
    service_type varchar(5),
    created_at datetime
);

insert into customers_raw (name, dob, created_at) values
('Antonio', '1998-08-04', '2025-03-01 14:24:40'),
('Brandon', '2001-04-21', '2025-03-02 08:12:54'),
('Charlie', '1980-11-15', '2025-03-02 11:20:02'),
('Dominikus', '1995-01-14', '2025-03-03 09:50:41'),
('Erik', '1900-01-01', '2025-03-03 17:22:03'),
('PT Black Bird', null, '2025-03-04 12:52:16');

insert into sales_raw (vin, customer_id, model, invoice_date, price, created_at) values
('JIS8135SAD', 1, 'RAIZA', '2025-03-01', '350.000.000', '2025-03-01 14:24:40'),
('MAS8160POE', 3, 'RANGGO', '2025-05-19', '430.000.000', '2025-05-19 14:29:21'),
('JLK1368KDE', 4, 'INNAVO', '2025-05-22', '600.000.000', '2025-05-22 16:10:28'),
('JLK1869KDF', 6, 'VELOS', '2025-08-02', '390.000.000', '2025-08-02 14:04:31'),
('JLK1962KOP', 6, 'VELOS', '2025-08-02', '390.000.000', '2025-08-02 15:21:04');

insert into after_sales_raw (service_ticket, vin, customer_id, model, service_date, service_type, created_at) values
('T124-kgu1', 'MAS8160POE', 3, 'RANGGO', '2025-07-11', 'BP', '2025-07-11 09:24:40'),
('T560-jga1', 'JLK1368KDE', 4, 'INNAVO', '2025-08-04', 'PM', '2025-08-04 10:12:54'),
('T521-oai8', 'POI1059IIK', 5, 'RAIZA', '2026-09-10', 'GR', '2026-09-10 12:45:02');