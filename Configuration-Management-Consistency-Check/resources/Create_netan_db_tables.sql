CREATE SEQUENCE if not exists "tblCMRules_seq";
CREATE TABLE if not exists "tblCMRules"(
    "RuleID" int PRIMARY KEY DEFAULT NEXTVAL ('"tblCMRules_seq"') NOT NULL,
    "RuleName" varchar(4000) NULL,
    "MOClass" varchar(400) NULL,
    "CMAttribute" varchar(400) NULL,
    "ID" varchar(40) NULL,
    "VectorIndex" int,
    "RuleValue" varchar(255) NULL,
    "RuleComment" varchar(400) NULL,
	"WhereClause" varchar(400) NULL,
	"RuleSource" varchar(400) NULL,
	"ValidationStatus" varchar(40) NULL,
    "InvalidCauseDescription" varchar(400) NULL,
	"TableName" varchar(255) NULL
);

CREATE SEQUENCE if not exists "tblExcludedNodes_seq";
CREATE TABLE if not exists "tblExcludedNodes"(
    "NodeID" int PRIMARY KEY DEFAULT NEXTVAL ('"tblExcludedNodes_seq"') NOT NULL,
    "NodeName" varchar(256) NULL
);