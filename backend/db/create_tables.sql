DROP TABLE IF EXISTS jobs_info CASCADE;
DROP TABLE IF EXISTS ds_meta CASCADE;
DROP TABLE IF EXISTS proj_ds_mapping CASCADE;
DROP TABLE IF EXISTS datasets_versions CASCADE;
DROP TABLE IF EXISTS datasets CASCADE;
DROP TABLE IF EXISTS projects CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS user_roles CASCADE;
DROP TABLE IF EXISTS modules CASCADE;
DROP TABLE IF EXISTS roles CASCADE;
DROP TABLE IF EXISTS session_history CASCADE;
DROP TABLE IF EXISTS local_exp CASCADE;
DROP TABLE IF EXISTS infrence_data CASCADE;
DROP TABLE IF EXISTS deployment_confs CASCADE;
DROP TABLE IF EXISTS deployment CASCADE;
DROP TABLE IF EXISTS model_tags CASCADE;
DROP TABLE IF EXISTS model_version CASCADE;
DROP TABLE IF EXISTS model_hub CASCADE;
DROP TABLE IF EXISTS global_exp CASCADE;
DROP TABLE IF EXISTS monitoring CASCADE;
DROP TABLE IF EXISTS monitoring_check CASCADE;
DROP TABLE IF EXISTS project_user CASCADE;
DROP TABLE IF EXISTS datasets_users CASCADE;
DROP TABLE IF EXISTS permissions CASCADE;
DROP TABLE IF EXISTS roles_permission_map CASCADE;
DROP TABLE IF EXISTS pdvu_mapping CASCADE;
DROP TABLE IF EXISTS user_persona CASCADE;
DROP TABLE IF EXISTS pii CASCADE;
DROP TABLE IF EXISTS auto_ml CASCADE;

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

CREATE TABLE modules
(
    module_id serial,
    module_name character varying(50) NOT NULL UNIQUE,
    PRIMARY KEY (module_id)
);

ALTER TABLE modules
    OWNER to pace_user;


CREATE TABLE permissions
(
    perm_id serial,
    perm_lvl int NOT NULL,
    module_id int,
    PRIMARY KEY (perm_id),
    CONSTRAINT fk_module
      FOREIGN KEY(module_id) 
	  REFERENCES modules(module_id),
    CONSTRAINT unique_perm_lvl_module_id
      UNIQUE(perm_lvl, module_id)   

);

ALTER TABLE permissions
    OWNER to pace_user;



CREATE TABLE roles
(
    role_id serial,
    creation_user_id int,
    role_name character varying(50) NOT NULL UNIQUE,
    is_deleted boolean DEFAULT FALSE,
    creation_time bigint NOT NULL,
    is_persona boolean NOT NULL,
    is_hidden boolean DEFAULT FALSE,
    last_modified bigint NOT NULL,
    PRIMARY KEY (role_id)
);

ALTER TABLE roles
    OWNER to pace_user;


CREATE TABLE roles_permission_map
(
    pk serial,
    role_id int,
    creation_user_id int,
    perm_id int NOT NULL,
    is_deleted boolean DEFAULT FALSE,
    creation_time bigint NOT NULL,
    last_modified bigint NOT NULL,
/*    PRIMARY KEY (creation_time),*/
    PRIMARY KEY (pk),
    CONSTRAINT fk_role_id
      FOREIGN KEY(role_id) 
	  REFERENCES roles(role_id),
    CONSTRAINT fk_perm_id
      FOREIGN KEY(perm_id) 
	  REFERENCES permissions(perm_id)

);

ALTER TABLE roles_permission_map
    OWNER to pace_user;


CREATE TABLE users
(
    user_id serial,
    name character varying(100) NOT NULL,
    email_id character varying(100) NOT NULL,
    login_id character varying(50) NOT NULL UNIQUE,
    pwd character varying(30) NOT NULL,
    license_id int,
    is_deleted boolean DEFAULT FALSE,
    creation_user_id int,
    creation_time bigint NOT NULL,
    last_modified bigint NOT NULL,
    PRIMARY KEY (user_id),
    CONSTRAINT fk_creation_user_id
      FOREIGN KEY(creation_user_id) 
	  REFERENCES users(user_id),
    CONSTRAINT fk_license_id
      FOREIGN KEY(license_id) 
	  REFERENCES license_issued(license_id)
	
);

ALTER TABLE users
    OWNER to pace_user;

ALTER TABLE roles
    ADD CONSTRAINT fk_user_id
		FOREIGN KEY(creation_user_id) 
		REFERENCES users(user_id);

ALTER TABLE roles_permission_map
    ADD CONSTRAINT fk_user_id
		FOREIGN KEY(creation_user_id) 
		REFERENCES users(user_id);


CREATE TABLE user_persona
(
    user_id int NOT NULL,
	role_id int NOT NULL,
    creation_user_id int,
    creation_time bigint NOT NULL,
    is_deleted boolean DEFAULT FALSE,
    last_modified bigint NOT NULL,
    CONSTRAINT fk_user_id
      FOREIGN KEY(user_id) 
	  REFERENCES users(user_id),
    CONSTRAINT fk_role_id
      FOREIGN KEY(role_id) 
	  REFERENCES roles(role_id),
    CONSTRAINT fk_creation_user_id
      FOREIGN KEY(creation_user_id) 
	  REFERENCES users(user_id)
);


ALTER TABLE user_persona
    OWNER to pace_user;


CREATE TABLE session_history
(
    user_id int NOT NULL,
	login_time bigint NOT NULL,
    session_token character varying(50) NOT NULL,
    CONSTRAINT fk_user_id
      FOREIGN KEY(user_id) 
	  REFERENCES users(user_id)
    
);

ALTER TABLE session_history
    OWNER to pace_user;	

/* ------------------------------------------------Insert for user management --------------------------------------------- */
INSERT INTO modules (module_id, module_name) VALUES (1, 'Data Preprocessing');
INSERT INTO modules (module_id, module_name) VALUES (2, 'Feature Engneering');
INSERT INTO modules (module_id, module_name) VALUES (3, 'Data Augumentation');
INSERT INTO modules (module_id, module_name) VALUES (4, 'Projects');
INSERT INTO modules (module_id, module_name) VALUES (5, 'Datasets');




INSERT INTO permissions (perm_lvl, module_id) VALUES (1, 1);
INSERT INTO permissions (perm_lvl, module_id) VALUES (2, 1);
INSERT INTO permissions (perm_lvl, module_id) VALUES (3, 1);
INSERT INTO permissions (perm_lvl, module_id) VALUES (1, 2);
INSERT INTO permissions (perm_lvl, module_id) VALUES (2, 2);
INSERT INTO permissions (perm_lvl, module_id) VALUES (3, 2);
INSERT INTO permissions (perm_lvl, module_id) VALUES (1, 3);
INSERT INTO permissions (perm_lvl, module_id) VALUES (2, 3);
INSERT INTO permissions (perm_lvl, module_id) VALUES (3, 3);
INSERT INTO permissions (perm_lvl, module_id) VALUES (1, 4);
INSERT INTO permissions (perm_lvl, module_id) VALUES (2, 4);
INSERT INTO permissions (perm_lvl, module_id) VALUES (3, 4);
INSERT INTO permissions (perm_lvl, module_id) VALUES (1, 5);
INSERT INTO permissions (perm_lvl, module_id) VALUES (2, 5);
INSERT INTO permissions (perm_lvl, module_id) VALUES (3, 5);


INSERT INTO users (name, email_id, login_id, pwd, is_deleted, creation_user_id, creation_time, last_modified) VALUES ('admin', 'admin@mphasis.com', 'admin', 'nextlabs@123', FALSE, NULL, 1625143, 1629197311);
INSERT INTO users (name, email_id, login_id, pwd, is_deleted, creation_user_id, creation_time, last_modified) VALUES ('Rahul', 'rahul@mphasis.com', 'rahul', 'nextlabs@123', FALSE, NULL, 1625143, 1629197311);
INSERT INTO users (name, email_id, login_id, pwd, is_deleted, creation_user_id, creation_time, last_modified) VALUES ('Himanshu', 'himanshu@mphasis.com', 'Himanshu', 'nextlabs@123', FALSE, NULL, 1625143, 1629197311);
INSERT INTO users (name, email_id, login_id, pwd, is_deleted, creation_user_id, creation_time, last_modified) VALUES ('Siddharth', 'Siddharth@mphasis.com', 'Siddharth', 'nextlabs@123', FALSE, NULL, 1625143, 1629197311);
INSERT INTO users (name, email_id, login_id, pwd, is_deleted, creation_user_id, creation_time, last_modified) VALUES ('Amrit', 'Amrit@mphasis.com', 'Amrit', 'nextlabs@123', FALSE, NULL, 1625143, 1629197311);
INSERT INTO users (name, email_id, login_id, pwd, is_deleted, creation_user_id, creation_time, last_modified) VALUES ('Ayaan', 'Amrit@mphasis.com', 'Ayaan', 'nextlabs@123', FALSE, NULL, 1625143, 1629197311);


INSERT INTO roles (role_id, creation_user_id, role_name, is_deleted, creation_time, is_persona, last_modified, is_hidden) VALUES (-1, NULL, 'Owner', false, 1625143, true, 1625380, true);
INSERT INTO roles (creation_user_id, role_name, is_deleted, creation_time, is_persona, last_modified) VALUES (NULL, 'Admin', false, 1625143, true, 1625380);
INSERT INTO roles (creation_user_id, role_name, is_deleted, creation_time, is_persona, last_modified) VALUES (NULL, 'Manager', false, 1625143, true, 1625290);
INSERT INTO roles (creation_user_id, role_name, is_deleted, creation_time, is_persona, last_modified) VALUES (NULL, 'Data Scientist', false, 1631275033, true, 1631275033);
INSERT INTO roles (creation_user_id, role_name, is_deleted, creation_time, is_persona, last_modified) VALUES (NULL, 'ML Engineer', false, 1631275033, true, 1631275033);


INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (1, NULL, 3, 1625143, 1625843);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (1, NULL, 6, 1625144, 1625743);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (1, NULL, 9, 1625145, 1625643);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (1, NULL, 12, 1625146, 1625543);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (1, NULL, 15, 1625147, 1625443);

INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (2, NULL, 3, 1625143, 1625843);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (2, NULL, 6, 1625144, 1625743);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (2, NULL, 9, 1625145, 1625643);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (2, NULL, 12, 1625146, 1625543);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (2, NULL, 15, 1625147, 1625443);

INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (3, NULL, 3, 1625143, 1625843);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (3, NULL, 6, 1625144, 1625743);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (3, NULL, 9, 1625145, 1625643);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (3, NULL, 12, 1625146, 1625543);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (3, NULL, 15, 1625147, 1625443);

INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (4, NULL, 3, 1625143, 1625843);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (4, NULL, 6, 1625144, 1625743);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (4, NULL, 9, 1625145, 1625643);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (4, NULL, 12, 1625146, 1625543);
INSERT INTO roles_permission_map (role_id, creation_user_id, perm_id, creation_time, last_modified) VALUES (4, NULL, 15, 1625147, 1625443);



INSERT INTO user_persona (user_id, role_id, creation_time, creation_user_id, is_deleted, last_modified) VALUES (1, 1, 1625143, 1, FALSE, 1629197311);
INSERT INTO user_persona (user_id, role_id, creation_time, creation_user_id, is_deleted, last_modified) VALUES (2, 2, 1625143, 1, FALSE, 1629197311);
INSERT INTO user_persona (user_id, role_id, creation_time, creation_user_id, is_deleted, last_modified) VALUES (3, 3, 1625143, 1, FALSE, 1629197311);
INSERT INTO user_persona (user_id, role_id, creation_time, creation_user_id, is_deleted, last_modified) VALUES (4, 3, 1625143, 1, FALSE, 1629197311);
INSERT INTO user_persona (user_id, role_id, creation_time, creation_user_id, is_deleted, last_modified) VALUES (5, 3, 1625143, 1, FALSE, 1629197311);
INSERT INTO user_persona (user_id, role_id, creation_time, creation_user_id, is_deleted, last_modified) VALUES (6, 3, 1625143, 1, FALSE, 1629197311);

CREATE TABLE projects
(
    project_id serial,
	name character varying(100) NOT NULL,
    proj_desc character varying(500) NOT NULL,
    creation_user_id int,
    creation_time bigint NOT NULL,
    mlflow_id int NOT NULL,
    is_deleted boolean DEFAULT False,
    last_modified bigint NOT NULL,
    PRIMARY KEY (project_id),
	CONSTRAINT fk_user_id
      FOREIGN KEY(creation_user_id) 
	  REFERENCES users(user_id)
);

ALTER TABLE projects
    OWNER to pace_user;


CREATE TABLE project_user
(
    pk serial,
    project_id int,
    user_id int,
    role_id int,
    creation_user_id int,
    creation_time bigint NOT NULL,
    is_deleted boolean DEFAULT False,
    last_modified bigint NOT NULL,
    PRIMARY KEY (pk),
    CONSTRAINT fk_proj_id
      FOREIGN KEY(project_id) 
	  REFERENCES projects(project_id),
    CONSTRAINT fk_user_id
      FOREIGN KEY(user_id) 
	  REFERENCES users(user_id),
    CONSTRAINT fk_role_id
      FOREIGN KEY(role_id) 
	  REFERENCES roles(role_id),
    CONSTRAINT fk_c_user_id
      FOREIGN KEY(creation_user_id) 
	  REFERENCES users(user_id)
);

ALTER TABLE project_user
    OWNER to pace_user;

CREATE TABLE datasets
(
    dataset_id serial,
	user_id int,
	name character varying(100) NOT NULL,
    ds_desc character varying(500) NOT NULL,
    creation_time bigint NOT NULL,
    completed boolean DEFAULT True,
    is_deleted boolean DEFAULT False,
    last_modified bigint NOT NULL,
    PRIMARY KEY (dataset_id),
	CONSTRAINT fk_user_id
      FOREIGN KEY(user_id) 
	  REFERENCES users(user_id)
);

ALTER TABLE datasets
    OWNER to pace_user;


CREATE TABLE proj_ds_mapping
(   
    pk serial,
    dataset_id int,
    project_id int,
    is_deleted boolean DEFAULT False,
    creation_time bigint NOT NULL,
    creation_user_id int,
    public boolean DEFAULT FALSE,
    last_modified bigint NOT NULL,
    PRIMARY KEY (pk),
    CONSTRAINT fk_dataset_id
      FOREIGN KEY(dataset_id) 
	  REFERENCES datasets(dataset_id),
	CONSTRAINT fk_project_id
      FOREIGN KEY(project_id) 
	  REFERENCES projects(project_id),
    CONSTRAINT fk_creation_user_id
       FOREIGN KEY(creation_user_id)
       REFERENCES users(user_id)
);

ALTER TABLE proj_ds_mapping
    OWNER to pace_user;


CREATE TABLE pii
(   
    pii_id serial,
    dataset_id int,
    is_deleted boolean DEFAULT False,
    pii_columns character varying(100),
    PRIMARY KEY (pii_id),
    CONSTRAINT fk_dataset_id
      FOREIGN KEY(dataset_id) 
	  REFERENCES datasets(dataset_id)
);

ALTER TABLE pii
    OWNER to pace_user;





CREATE TABLE datasets_versions
(
    version_id serial,
	dataset_id int,
	user_id int,
    creation_time bigint NOT NULL,
    prev_id int DEFAULT -1,                 /* previous version id */
	location character varying(100) NOT NULL,
	job_id int DEFAULT -1,
    is_deleted bool DEFAULT False,
    profiling_done boolean DEFAULT False,
    profiling_job_id int DEFAULT -1,
    PRIMARY KEY (dataset_id,version_id),
	CONSTRAINT fk_user_id
      FOREIGN KEY(user_id) 
	  REFERENCES users(user_id),
	CONSTRAINT fk_dataset_id
      FOREIGN KEY(dataset_id) 
	  REFERENCES datasets(dataset_id)
);

ALTER TABLE datasets_versions
    OWNER to pace_user;


CREATE TABLE pdvu_mapping
(	
	pk serial,
    version_id int NOT NULL,
	dataset_id int NOT NULL,
	project_user_id int NOT NULL,
    project_ds_id int NOT NULL,
	creation_time bigint NOT NULL,
    is_current bool DEFAULT True,
    last_modified bigint NOT NULL,
    target_col CHARACTER VARYING(100),
    PRIMARY KEY (pk),
	CONSTRAINT fk_dataset_vs_id
      FOREIGN KEY(dataset_id,version_id) 
	  REFERENCES datasets_versions(dataset_id,version_id),
	CONSTRAINT fk_proj_user_id
      FOREIGN KEY(project_user_id) 
	  REFERENCES project_user(pk),
    CONSTRAINT fk_proj_ds_id
      FOREIGN KEY(project_ds_id) 
	  REFERENCES proj_ds_mapping(pk)
);

ALTER TABLE pdvu_mapping
    OWNER to pace_user;


CREATE TABLE ds_meta
(
    version_id int,
	dataset_id int,
	row_count int NOT NULL,
    col_count bigint NOT NULL,
    col_info json NOT NULL,
	ds_size int NOT NULL,
    is_deleted bool DEFAULT False,
    PRIMARY KEY (dataset_id,version_id),
	CONSTRAINT fk_dataset_id
      FOREIGN KEY(dataset_id,version_id) 
	  REFERENCES datasets_versions(dataset_id,version_id)
);

ALTER TABLE ds_meta
    OWNER to pace_user;

CREATE TABLE jobs_info
(   
    job_id serial,
    pdvu_id int NOT NULL,
    creation_time BIGINT NOT NULL,
    start_time BIGINT NOT NULL,
    end_time BIGINT NOT NULL,
    job_status int DEFAULT 0,
    job_options json NOT NULL,
    job_type character VARYING(100) NOT NULL,
    parent_job_id int DEFAULT -1,
    is_deleted bool DEFAULT False,
    PRIMARY KEY (job_id),
	CONSTRAINT fk_pdvu_id
      FOREIGN KEY(pdvu_id) 
	  REFERENCES pdvu_mapping(pk)
);

ALTER TABLE jobs_info
    OWNER to pace_user;


CREATE TABLE auto_ml
(   
    auto_ml_id serial,
    job_id int NOT NULL,
    auto_ml_type CHARACTER VARYING(50),
    leadboard_loc  CHARACTER VARYING(200),
    is_deleted bool DEFAULT False,
    PRIMARY KEY (auto_ml_id),
	CONSTRAINT fk_job_id
      FOREIGN KEY(job_id) 
	  REFERENCES jobs_info(job_id)
);

ALTER TABLE auto_ml
    OWNER to pace_user;


CREATE TABLE global_exp
(
    job_id int,
    variable CHARACTER VARYING(50),
    exp_value CHARACTER VARYING(50),
    PRIMARY KEY (job_id, variable),
    CONSTRAINT fk_pjobs_info
        FOREIGN KEY(job_id) 
        REFERENCES jobs_info(job_id)
);

ALTER TABLE global_exp
    OWNER to pace_user;


CREATE TABLE model_hub
(
    model_id serial,
    model_name CHARACTER VARYING(50),
    project_id int,
    user_id int,
    visibility BOOLEAN,
    is_deleted boolean,
    PRIMARY KEY (model_id),
	CONSTRAINT fk_proj_mapping
      FOREIGN KEY(project_id) 
	  REFERENCES projects(project_id),
    CONSTRAINT fk_user_id
      FOREIGN KEY(user_id) 
	  REFERENCES users(user_id)
);

ALTER TABLE model_hub
    OWNER to pace_user;


CREATE TABLE model_version
(
    model_id int,
    version_id serial,
    model_version_name CHARACTER VARYING(100)
    mlflow_runid CHARACTER VARYING(50),
    job_id int NOT NULL,
    creation_time bigint NOT NULL,
    model_param json,
    model_hyperparameters json,
    pipeline_dict json,
    is_deleted boolean,
    PRIMARY KEY (version_id,model_id),
    CONSTRAINT fk_model_mapping
      FOREIGN KEY(model_id) 
	  REFERENCES model_hub(model_id),
    CONSTRAINT fk_job_id
      FOREIGN KEY(job_id) 
	  REFERENCES jobs_info(job_id)
);

ALTER TABLE model_version
    OWNER to pace_user;


CREATE TABLE model_tags
(
    model_id int,
    version_id int,
    tag CHARACTER VARYING(50),
    tag_value CHARACTER VARYING(50),
    PRIMARY KEY (model_id,version_id,tag),
    CONSTRAINT fk_model_mapping
      FOREIGN KEY(model_id, version_id) 
	  REFERENCES model_version(model_id,version_id)
);

ALTER TABLE model_tags
    OWNER to pace_user;


CREATE TABLE deployment
(
    d_id serial,
    name CHARACTER VARYING(100),
    model_id int,
    version_id int,
    user_id int,
    status CHARACTER VARYING(50),
    last_modified bigint,
    last_modified_by int,
    creation_time bigint NOT NULL,
    access_lvl CHARACTER VARYING(50),
    end_point CHARACTER VARYING(50),
    is_deleted boolean,
    PRIMARY KEY (d_id),
    CONSTRAINT fk_model_mapping
      FOREIGN KEY(model_id, version_id) 
	  REFERENCES model_version(model_id,version_id),
    CONSTRAINT fk_user_id
      FOREIGN KEY(user_id) 
	  REFERENCES users(user_id),
    CONSTRAINT fk_last_m_by
      FOREIGN KEY(last_modified_by) 
	  REFERENCES users(user_id)

);

ALTER TABLE deployment
    OWNER to pace_user;


CREATE TABLE deployment_confs
(
    d_id int,
    tag CHARACTER VARYING(50),
    tag_value CHARACTER VARYING(50),
    PRIMARY KEY(d_id, tag),
    CONSTRAINT fk_deployment_id
      FOREIGN KEY(d_id) 
	  REFERENCES deployment(d_id)

);


ALTER TABLE deployment_confs
    OWNER to pace_user;



CREATE TABLE infrence_data 
(
    data_id serial,
    d_id int,
    data_value json,
    PRIMARY KEY(data_id),
    CONSTRAINT fk_deployment_id
      FOREIGN KEY(d_id) 
	  REFERENCES deployment(d_id)
);

ALTER TABLE infrence_data
    OWNER to pace_user;


CREATE TABLE local_exp
(
    data_id int,
    variable CHARACTER VARYING(50),
    exp_value CHARACTER VARYING(50),
    PRIMARY KEY (data_id, variable),
    CONSTRAINT fk_data_id
        FOREIGN KEY(data_id)
        REFERENCES infrence_data(data_id)
);

ALTER TABLE local_exp
    OWNER to pace_user;


CREATE TABLE monitoring
(
    d_id int,
    time_stamp BIGINT,
    tag CHARACTER VARYING(50),
    tag_value CHARACTER VARYING(50),
    CONSTRAINT fk_deployment_id
      FOREIGN KEY(d_id) 
	  REFERENCES deployment(d_id)
);

ALTER TABLE monitoring
    OWNER to pace_user;


CREATE TABLE monitoring_check
(
    d_id int,
    tag CHARACTER VARYING(50),
    tag_c_value CHARACTER VARYING(50),
    CONSTRAINT fk_deployment_id
      FOREIGN KEY(d_id) 
	  REFERENCES deployment(d_id)

);

ALTER TABLE monitoring_check
    OWNER to pace_user;

