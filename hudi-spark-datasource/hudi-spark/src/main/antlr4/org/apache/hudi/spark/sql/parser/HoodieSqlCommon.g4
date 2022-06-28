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

 grammar HoodieSqlCommon;

 @lexer::members {
  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }
}

 singleStatement
    : statement ';'* EOF
    ;

 statement
    : compactionStatement                                                       #compactionCommand
    | CALL multipartIdentifier '(' (callArgument (',' callArgument)*)? ')'      #call
    | CREATE INDEX (IF NOT EXISTS)? identifier ON TABLE?
          tableIdentifier (USING indexType=identifier)?
          LEFT_PAREN columns=multipartIdentifierPropertyList RIGHT_PAREN
          (OPTIONS indexOptions=propertyList)?                                  #createIndex
    | DROP INDEX (IF EXISTS)? identifier ON TABLE? tableIdentifier              #dropIndex
    | SHOW INDEXES (FROM | IN) TABLE? tableIdentifier                           #showIndexes
    | REFRESH INDEX identifier ON TABLE? tableIdentifier                        #refreshIndex
    | .*?                                                                       #passThrough
    ;

 compactionStatement
    : operation = (RUN | SCHEDULE) COMPACTION  ON tableIdentifier (AT instantTimestamp = INTEGER_VALUE)?    #compactionOnTable
    | operation = (RUN | SCHEDULE) COMPACTION  ON path = STRING   (AT instantTimestamp = INTEGER_VALUE)?    #compactionOnPath
    | SHOW COMPACTION  ON tableIdentifier (LIMIT limit = INTEGER_VALUE)?                             #showCompactionOnTable
    | SHOW COMPACTION  ON path = STRING (LIMIT limit = INTEGER_VALUE)?                               #showCompactionOnPath
    ;

 tableIdentifier
    : (db=IDENTIFIER '.')? table=IDENTIFIER
    ;

 callArgument
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;

 expression
    : constant
    | stringMap
    ;

 constant
    : number                          #numericLiteral
    | booleanValue                    #booleanLiteral
    | STRING+                         #stringLiteral
    | identifier STRING               #typeConstructor
    ;

 stringMap
    : MAP '(' constant (',' constant)* ')'
    ;

 booleanValue
    : TRUE | FALSE
    ;

 number
    : MINUS? EXPONENT_VALUE           #exponentLiteral
    | MINUS? DECIMAL_VALUE            #decimalLiteral
    | MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? BIGINT_LITERAL           #bigIntLiteral
    | MINUS? SMALLINT_LITERAL         #smallIntLiteral
    | MINUS? TINYINT_LITERAL          #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL           #doubleLiteral
    | MINUS? FLOAT_LITERAL            #floatLiteral
    | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
    ;

 multipartIdentifierPropertyList
     : multipartIdentifierProperty (COMMA multipartIdentifierProperty)*
     ;

 multipartIdentifierProperty
     : multipartIdentifier (OPTIONS options=propertyList)?
     ;

 multipartIdentifier
    : parts+=identifier ('.' parts+=identifier)*
    ;

 identifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | nonReserved             #unquotedIdentifier
    ;

 quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

 nonReserved
     : CALL
     | COMPACTION
     | CREATE
     | DROP
     | EXISTS
     | FROM
     | IN
     | INDEX
     | INDEXES
     | IF
     | LIMIT
     | NOT
     | ON
     | OPTIONS
     | REFRESH
     | RUN
     | SCHEDULE
     | SHOW
     | TABLE
     | USING
     ;

 propertyList
     : LEFT_PAREN property (COMMA property)* RIGHT_PAREN
     ;

 property
     : key=propertyKey (EQ? value=propertyValue)?
     ;

 propertyKey
     : identifier (DOT identifier)*
     | STRING
     ;

 propertyValue
     : INTEGER_VALUE
     | DECIMAL_VALUE
     | booleanValue
     | STRING
     ;

 LEFT_PAREN: '(';
 RIGHT_PAREN: ')';
 COMMA: ',';
 DOT: '.';

 ALL: 'ALL';
 AT: 'AT';
 CALL: 'CALL';
 COMPACTION: 'COMPACTION';
 RUN: 'RUN';
 SCHEDULE: 'SCHEDULE';
 ON: 'ON';
 SHOW: 'SHOW';
 LIMIT: 'LIMIT';
 MAP: 'MAP';
 NULL: 'NULL';
 TRUE: 'TRUE';
 FALSE: 'FALSE';
 INTERVAL: 'INTERVAL';
 TO: 'TO';
 CREATE: 'CREATE';
 INDEX: 'INDEX';
 INDEXES: 'INDEXES';
 IF: 'IF';
 NOT: 'NOT';
 EXISTS: 'EXISTS';
 TABLE: 'TABLE';
 USING: 'USING';
 OPTIONS: 'OPTIONS';
 DROP: 'DROP';
 FROM: 'FROM';
 IN: 'IN';
 REFRESH: 'REFRESH';

 EQ: '=' | '==';

 PLUS: '+';
 MINUS: '-';

 STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

 BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

 SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

 TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

 INTEGER_VALUE
    : DIGIT+
    ;

 EXPONENT_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT {isValidDecimal()}?
    ;

 DECIMAL_VALUE
    : DECIMAL_DIGITS {isValidDecimal()}?
    ;

 FLOAT_LITERAL
    : DIGIT+ EXPONENT? 'F'
    | DECIMAL_DIGITS EXPONENT? 'F' {isValidDecimal()}?
    ;

 DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

 BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

 IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

 BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

 fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

 fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

 fragment DIGIT
    : [0-9]
    ;

 fragment LETTER
    : [A-Z]
    ;

 SIMPLE_COMMENT
     : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
     ;

 BRACKETED_COMMENT
     : '/*' .*? '*/' -> channel(HIDDEN)
     ;

 WS  : [ \r\n\t]+ -> channel(HIDDEN)
     ;

 // Catch-all for anything we can't recognize.
 // We use this to be able to ignore and recover all the text
 // when splitting statements with DelimiterLexer
 UNRECOGNIZED
     : .
     ;
