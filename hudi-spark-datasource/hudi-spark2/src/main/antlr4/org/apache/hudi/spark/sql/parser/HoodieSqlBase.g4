/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar HoodieSqlBase;

import SqlBase;

singleStatement
    : statement EOF
    ;

statement
    : mergeInto                                                        #mergeIntoTable
    | updateTableStmt                                                  #updateTable
    | deleteTableStmt                                                  #deleteTable
    | CREATE INDEX indexName=identifier
          ON tableIdentifier ('(' indexColumn=identifier ')')
          AS indexType=identifier                                      #createIndex
    | SHOW INDEXES (FROM | IN) tableIdentifier                         #showIndex
    | DROP INDEX indexName=identifier ON tableIdentifier               #dropIndex
    | .*?                                                              #passThrough
    ;


mergeInto
    : MERGE INTO target=tableIdentifier tableAlias
      USING (source=tableIdentifier | '(' subquery = query ')') tableAlias
      mergeCondition
      matchedClauses*
      notMatchedClause*
    ;

mergeCondition
    : ON condition=booleanExpression
    ;

matchedClauses
    : deleteClause
    | updateClause
    ;

notMatchedClause
    : insertClause
    ;

deleteClause
    : WHEN MATCHED (AND deleteCond=booleanExpression)? THEN deleteAction
    | WHEN deleteCond=booleanExpression THEN deleteAction
    ;

updateClause
    : WHEN MATCHED (AND updateCond=booleanExpression)? THEN updateAction
    | WHEN updateCond=booleanExpression THEN updateAction
    ;

insertClause
    : WHEN NOT MATCHED (AND insertCond=booleanExpression)? THEN insertAction
    | WHEN insertCond=booleanExpression THEN insertAction
    ;
deleteAction
    : DELETE
    ;

updateAction
    : UPDATE SET ASTERISK
    | UPDATE SET assignmentList
    ;

insertAction
    : INSERT ASTERISK
    | INSERT '(' columns=qualifiedNameList ')' VALUES '(' expression (',' expression)* ')'
    ;

assignmentList
    : assignment (',' assignment)*
    ;

assignment
    : key=qualifiedName EQ value=expression
    ;

qualifiedNameList
    : qualifiedName (',' qualifiedName)*
    ;

updateTableStmt
  : UPDATE tableIdentifier SET assignmentList (WHERE where=booleanExpression)?
  ;

deleteTableStmt
  : DELETE FROM tableIdentifier (WHERE where=booleanExpression)?
  ;


PRIMARY: 'PRIMARY';
KEY: 'KEY';
MERGE: 'MERGE';
MATCHED: 'MATCHED';
UPDATE: 'UPDATE';
