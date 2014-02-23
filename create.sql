CREATE TABLE account (
    id INT,
    username CHAR(25) PRIMARY KEY,
    journaltype CHAR(1),
    fetched INT DEFAULT 0,
    failed INT DEFAULT 1,
    fetched_ints INT DEFAULT 0
);
CREATE UNIQUE INDEX acct_id ON account (id);
CREATE INDEX acct_jtype ON account (journaltype);
CREATE INDEX acct_fetched ON account (fetched);
CREATE INDEX acct_fetched_ints ON account (fetched_ints);

CREATE TABLE account_errors (
    username CHAR(25) PRIMARY KEY,
    error TEXT
);

CREATE TABLE edges (
    edge CHAR(20),
    src INT,
    dest INT,
    PRIMARY KEY(edge,src,dest)
);
