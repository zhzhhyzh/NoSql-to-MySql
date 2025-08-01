CREATE TABLE time_slot (
    time_slot_id CHAR(1),
    day CHAR(1),
    start_hr INT,
    start_min INT,
    end_hr INT,
    end_min INT,
    PRIMARY KEY (time_slot_id, day)
);
CREATE TABLE classroom (
    building VARCHAR(50),
    room_number VARCHAR(10),
    capacity INT,
    PRIMARY KEY (building, room_number)
);
CREATE TABLE department (
    dept_name VARCHAR(50) PRIMARY KEY,
    building VARCHAR(50),
    budget DECIMAL(12,2),
    FOREIGN KEY (building) REFERENCES classroom(building)

);

CREATE TABLE instructor (
    ID VARCHAR(10) PRIMARY KEY,
    name VARCHAR(50),
    salary DECIMAL(10,2),
    dept_name VARCHAR(50),
    FOREIGN KEY (dept_name) REFERENCES department(dept_name)
);

CREATE TABLE student (
    ID VARCHAR(10) PRIMARY KEY,
    name VARCHAR(50),
    tot_cred INT,
    dept_name VARCHAR(50),
    FOREIGN KEY (dept_name) REFERENCES department(dept_name)
);

CREATE TABLE course (
    course_id VARCHAR(10) PRIMARY KEY,
    title VARCHAR(100),
    dept_name VARCHAR(50),
    credits INT,
    FOREIGN KEY (dept_name) REFERENCES department(dept_name)
);
CREATE TABLE section (
    course_id VARCHAR(10),
    sec_id VARCHAR(10),
    semester VARCHAR(10),
    PRIMARY KEY (course_id, sec_id, semester),
    FOREIGN KEY (course_id) REFERENCES course(course_id)
);
CREATE TABLE teaches (
    ID VARCHAR(10),
    course_id VARCHAR(10),
    sec_id VARCHAR(10),
    semester VARCHAR(10),
    year INT,
    PRIMARY KEY (ID, course_id, sec_id),
    FOREIGN KEY (ID) REFERENCES instructor(ID),
    FOREIGN KEY (course_id, sec_id) REFERENCES section(course_id, sec_id)
);

CREATE TABLE prereq (
    course_id VARCHAR(10),
    prereq_id VARCHAR(10),
    PRIMARY KEY (course_id, prereq_id),
    FOREIGN KEY (course_id) REFERENCES course(course_id)
    FOREIGN KEY (prereq_id) REFERENCES course(course_id)
);

CREATE TABLE advisor (
    i_id VARCHAR(10),
    s_id VARCHAR(10),
    PRIMARY KEY (i_id, s_id),
    FOREIGN KEY (i_id) REFERENCES instructor(ID)
    FOREIGN KEY (s_id) REFERENCES student(ID)
);

CREATE TABLE takes (
    ID VARCHAR(10),
    course_id VARCHAR(10),
    sec_id VARCHAR(10),
    semester VARCHAR(10),
    year INT,
    grade VARCHAR(2),
    PRIMARY KEY (ID, course_id, sec_id),
    FOREIGN KEY (ID) REFERENCES student(ID),
    FOREIGN KEY (course_id, sec_id) REFERENCES section(course_id, sec_id)
);

