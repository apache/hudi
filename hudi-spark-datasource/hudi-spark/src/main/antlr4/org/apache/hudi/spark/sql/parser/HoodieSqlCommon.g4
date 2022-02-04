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

 @members {
    /**
     * When false, INTERSECT is given the greater precedence over the other set
     * operations (UNION, EXCEPT and MINUS) as per the SQL standard.
     */
    public boolean legacy_setops_precedence_enbled = false;

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
    : statement EOF
    ;

 statement
    : compactionStatement                                              #compactionCommand
    | callStatement                                                    #callProcedure
    | .*?                                                              #passThrough
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

 callStatement
   : CALL multipartIdentifier '(' (callArgument (',' callArgument)*)? ')'  #call
   ;

 multipartIdentifier
     : parts+=identifier ('.' parts+=identifier)*
     ;

 callExpression
     : constant
     | stringMap
     ;

 callArgument
     : callExpression                        #positionalArgument
     | identifier '=>' callExpression        #namedArgument
     ;

 stringMap
     : MAP '(' constant (',' constant)* ')'
     ;

 identifier
     : strictIdentifier
     ;

 strictIdentifier
     : IDENTIFIER             #unquotedIdentifier
     | quotedIdentifier       #quotedIdentifierAlternative
     | nonReserved            #unquotedIdentifier
     ;

 quotedIdentifier
     : BACKQUOTED_IDENTIFIER
     ;

 constant
     : NULL                                                                                     #nullLiteral
     | interval                                                                                 #intervalLiteral
     | identifier STRING                                                                        #typeConstructor
     | number                                                                                   #numericLiteral
     | booleanValue                                                                             #booleanLiteral
     | STRING+                                                                                  #stringLiteral
     ;

 booleanValue
      : TRUE | FALSE
      ;

 interval
     : INTERVAL intervalField*
     ;

 intervalField
     : value=intervalValue unit=identifier (TO to=identifier)?
     ;

 intervalValue
     : (PLUS | MINUS)? (INTEGER_VALUE | DECIMAL_VALUE)
     | STRING
     ;

 number
     : MINUS? INTEGER_VALUE            #integerLiteral
     | MINUS? DECIMAL_VALUE            #decimalLiteral
     | MINUS? BIGINT_LITERAL           #bigIntLiteral
     | MINUS? SMALLINT_LITERAL         #smallIntLiteral
     | MINUS? TINYINT_LITERAL          #tinyIntLiteral
     | MINUS? DOUBLE_LITERAL           #doubleLiteral
     | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
     ;

 nonReserved
     : CALL | COMPACTION | RUN | SCHEDULE | ON | SHOW | LIMIT
     ;

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

 PLUS: '+';
 MINUS: '-';

 IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

 STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

 BACKQUOTED_IDENTIFIER
     : '`' ( ~'`' | '``' )* '`'
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

 DECIMAL_VALUE
     : DIGIT+ EXPONENT
     | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
     ;

 DOUBLE_LITERAL
     : DIGIT+ EXPONENT? 'D'
     | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
     ;

 BIGDECIMAL_LITERAL
     : DIGIT+ EXPONENT? 'BD'
     | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
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
