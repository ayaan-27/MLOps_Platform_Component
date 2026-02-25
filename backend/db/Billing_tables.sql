DROP TABLE IF EXISTS organization CASCADE;
DROP TABLE IF EXISTS account_recievable_details CASCADE;
DROP TABLE IF EXISTS account_recievable CASCADE;
DROP TABLE IF EXISTS payment_modes CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS distribution_modes CASCADE;
DROP TABLE IF EXISTS license_type CASCADE;
DROP TABLE IF EXISTS license_issued CASCADE;
DROP TABLE IF EXISTS payment_perlicense_details CASCADE;
DROP TABLE IF EXISTS discount_type CASCADE;
DROP TABLE IF EXISTS discount_issued CASCADE;
DROP TABLE IF EXISTS licenses_per_organization_details CASCADE;




CREATE TABLE license_type
(
    license_type_id serial,
    license_type character varying(100),
    cost int NOT NUll,
    duration int NOT NUll,
    max_users int NOT NUll,
    PRIMARY KEY (license_type_id)
);

ALTER TABLE license_type
    OWNER to pace_user;


CREATE TABLE payment_modes
(
    payment_modes serial,
    pm_desc character varying(100),
    PRIMARY KEY (payment_modes)
);

ALTER TABLE payment_modes
    OWNER to pace_user;

CREATE TABLE discount_type
(
    discount_type serial,
    discount_desc character varying(100),
    PRIMARY KEY (discount_type)
);

ALTER TABLE discount_type
    OWNER to pace_user;


CREATE TABLE organization
(
    org_id serial,
    name character varying(100) NOT NUll,
    primary_contact character varying(100) NOT NULL,
    secondary_contact character varying(100),
    address character varying(100) NOT NULL,
    city character varying(100) NOT NULL,
    state character varying(100),
    country character varying(100) NOT NULL,
    zip int NOT NULL,
    PRIMARY KEY (org_id)
);

ALTER TABLE organization
    OWNER to pace_user;

CREATE TABLE discount_issued
(
    discount_id serial,
    date_applied bigint,
    discount_type int,
    discount_value int,
    PRIMARY KEY (discount_id),
    CONSTRAINT fk_discount_type
      FOREIGN KEY(discount_type) 
	  REFERENCES discount_type(discount_type)
);


ALTER TABLE discount_issued
    OWNER to pace_user;



CREATE TABLE license_issued
(
    license_id serial,
    org_id int, 
    issued_date bigint NOT NUll,
    license_type_id int NOT NUll,
    discount_id int,
    PRIMARY KEY (license_id),
    CONSTRAINT fk_org_id
      FOREIGN KEY(org_id) 
	  REFERENCES organization(org_id),
    CONSTRAINT fk_license_type_id
      FOREIGN KEY(license_type_id) 
	  REFERENCES license_type(license_type_id),
    CONSTRAINT fk_discount_id
      FOREIGN KEY(discount_id) 
	  REFERENCES discount_issued(discount_id) 
);

ALTER TABLE license_issued
    OWNER to pace_user;


CREATE TABLE payments
(
    payment_id serial,
    payment_date bigint NOT NULL,
    payment_modes int NOT NULL,
    amt_received int NOT NULL,
    license_id int NOT NULL,
    voucher_number bigint,
    PRIMARY KEY (payment_id),
    CONSTRAINT fk_payment_modes
      FOREIGN KEY(payment_modes) 
	  REFERENCES payment_modes(payment_modes),
    CONSTRAINT fk_license_id
      FOREIGN KEY(license_id) 
	  REFERENCES license_issued(license_id)  
);

ALTER TABLE payments
    OWNER to pace_user;
