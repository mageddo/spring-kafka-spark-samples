CREATE TABLE USER (
    IDT_USER BIGINT,
    NAM_USER TEXT,
    PRIMARY KEY ((IDT_USER), NAM_USER)
)
WITH CLUSTERING ORDER BY (NAM_USER DESC);

INSERT INTO USER (IDT_USER, NAM_USER) VALUES (1, 'Elvis');
INSERT INTO USER (IDT_USER, NAM_USER) VALUES (2, 'Bruna');
