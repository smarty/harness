CREATE TABLE Snapshots (
  id               bigint unsigned NOT NULL AUTO_INCREMENT,
  created          datetime(3)     NOT NULL,
  high_watermark   bigint unsigned NOT NULL,
  payload          longblob        NOT NULL,
  content_type     varchar(127)    NOT NULL DEFAULT '',
  content_encoding varchar(31)     NOT NULL DEFAULT '',
  PRIMARY KEY (id)
);

CREATE TABLE Messages (
    id         bigint unsigned AUTO_INCREMENT NOT NULL,
    dispatched datetime(3)                        NULL,
    type       varchar(256)                   NOT NULL,
    payload    mediumblob                     NOT NULL,
    PRIMARY KEY (id)
);
CREATE UNIQUE INDEX ix_messages_dispatched ON Messages (dispatched, id);
