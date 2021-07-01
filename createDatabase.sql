# set up the database
CREATE DATABASE rrg;
USE rrg;

# create the tables
CREATE TABLE sector (
    sector_code CHAR(3) NOT NULL PRIMARY KEY,
    sector_name VARCHAR(30) NOT NULL
);

CREATE TABLE stock (
    stock_code VARCHAR(6) NOT NULL PRIMARY KEY,
    stock_name VARCHAR(30) NOT NULL,
    sector_code CHAR(3) NOT NULL,
    CONSTRAINT fk_sector FOREIGN KEY (sector_code)
    REFERENCES sector(sector_code)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE TABLE weekly_stock_quotes (
    quote_date DATE NOT NULL,
    stock_code VARCHAR(6) NOT NULL,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume INT,
    PRIMARY KEY(quote_date, stock_code),
    CONSTRAINT fk_stock FOREIGN KEY (stock_code)
    REFERENCES stock(stock_code)
    ON DELETE CASCADE
    ON UPDATE CASCADE    
);

CREATE TABLE weekly_sector_quotes (
    quote_date DATE NOT NULL,
    sector_code VARCHAR(6) NOT NULL,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume INT,
    PRIMARY KEY(quote_date, sector_code),
    CONSTRAINT fk_wkly_sector FOREIGN KEY (sector_code)
    REFERENCES sector(sector_code)
    ON DELETE CASCADE
    ON UPDATE CASCADE    
);


# bootstrap data
INSERT INTO sector (sector_code, sector_name) values ('xej', 'Energy');
INSERT INTO sector (sector_code, sector_name) values ('xmj', 'Materials');
INSERT INTO sector (sector_code, sector_name) values ('xnj', 'Industrials');
INSERT INTO sector (sector_code, sector_name) values ('xdj', 'Consumer Discretionary');
INSERT INTO sector (sector_code, sector_name) values ('xsj', 'Consumer Staples');
INSERT INTO sector (sector_code, sector_name) values ('xhj', 'Health Care');
INSERT INTO sector (sector_code, sector_name) values ('xfj', 'Financials');
INSERT INTO sector (sector_code, sector_name) values ('xij', 'Information Technology');
INSERT INTO sector (sector_code, sector_name) values ('xtj', 'Communications Services');
INSERT INTO sector (sector_code, sector_name) values ('xuj', 'Utilities');
INSERT INTO sector (sector_code, sector_name) values ('xpj', 'Real Estate');