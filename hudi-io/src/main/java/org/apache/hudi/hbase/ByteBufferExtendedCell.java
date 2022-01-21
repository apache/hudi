/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hbase;

import org.apache.yetus.audience.InterfaceAudience;

import java.nio.ByteBuffer;


/**
 * This class is a server side extension to the {@link Cell} interface. It is used when the
 * Cell is backed by a {@link ByteBuffer}: i.e. <code>cell instanceof ByteBufferedCell</code>.
 *
 * <p>This class has getters for the row, column family, column qualifier, value and tags hosting
 * ByteBuffers. It also has getters of the *position* within a ByteBuffer where these
 * field bytes begin. These are needed because a single ByteBuffer may back one or many Cell
 * instances -- it depends on the implementation -- so the ByteBuffer position as returned by
 * {@link ByteBuffer#arrayOffset()} cannot be relied upon. Also, do not confuse these position
 * methods with the getXXXOffset methods from the super Interface, {@link Cell}; dependent up on
 * implementation, the Cell getXXXOffset methods can return the same value as a call to its
 * equivalent position method from below BUT they can also stray; if a ByteBufferedCell, use the
 * below position methods to find where a field begins.
 *
 * <p>Use the getXXXLength methods from Cell to find a fields length.
 *
 * <p>A Cell object can be of this type only on the server side.
 *
 * <p>WARNING: If a Cell is backed by an offheap ByteBuffer, any call to getXXXArray() will result
 * in a temporary byte array creation and a bytes copy. Avoid these allocations by using the
 * appropriate Cell access server-side: i.e. ByteBufferedCell when backed by a ByteBuffer and Cell
 * when it is not.
 */
/*
 * Even though all the methods are abstract, ByteBufferExtendedCell is not made to be an interface
 * with intent. In CellComparator compare method, we have instance of check to decide whether to
 * use getXXXArray() or getXXXByteBuffer(). This is a very hot method in read and write paths.
 * if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      ....
    }
    if (left instanceof ByteBufferExtendedCell) {
      ....
    }
    if (right instanceof ByteBufferExtendedCell) {
      ....
    }
    return Bytes.compareTo(left.getRowArray(), left.getRowOffset(), left.getRowLength(),
        right.getRowArray(), right.getRowOffset(), right.getRowLength());
 * We did JMH micro benchmark tests with both left and right cells as ByteBufferExtendedCell, one
 * only ByteBufferExtendedCell and both as Cells. This is compared against JMH results on compare
 * logic with out any instance of checks. We noticed that if ByteBufferExtendedCell is an
 * interface, the benchmark result seems to be very bad for case of both right and left are Cell
 * only (Not ByteBufferExtendedCell). When ByteBufferExtendedCell is an abstract class all 4
 * possible cases giving almost similar performance number compared with compare logic with no
 * instance of checks.
 */
@InterfaceAudience.Private
public abstract class ByteBufferExtendedCell implements ExtendedCell {
  /**
   * @return The {@link ByteBuffer} containing the row bytes.
   */
  public abstract ByteBuffer getRowByteBuffer();

  /**
   * @return Position in the {@link ByteBuffer} where row bytes start
   */
  public abstract int getRowPosition();

  /**
   * @return The {@link ByteBuffer} containing the column family bytes.
   */
  public abstract ByteBuffer getFamilyByteBuffer();

  /**
   * @return Position in the {@link ByteBuffer} where column family bytes start
   */
  public abstract int getFamilyPosition();

  /**
   * @return The {@link ByteBuffer} containing the column qualifier bytes.
   */
  public abstract ByteBuffer getQualifierByteBuffer();

  /**
   * @return Position in the {@link ByteBuffer} where column qualifier bytes start
   */
  public abstract int getQualifierPosition();

  /**
   * @return The {@link ByteBuffer} containing the value bytes.
   */
  public abstract ByteBuffer getValueByteBuffer();

  /**
   * @return Position in the {@link ByteBuffer} where value bytes start
   */
  public abstract int getValuePosition();

  /**
   * @return The {@link ByteBuffer} containing the tag bytes.
   */
  public abstract ByteBuffer getTagsByteBuffer();

  /**
   * @return Position in the {@link ByteBuffer} where tag bytes start
   */
  public abstract int getTagsPosition();
}
