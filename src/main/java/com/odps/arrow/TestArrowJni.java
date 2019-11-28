package com.odps.arrow;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by niki.lj on 2019/8/31.
 */
public class TestArrowJni {

  public static BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

  public static native void javaToNative(byte[] schema, long[] bufAddrs, long[] bufSizes, int valueCount);

  public static native DataNode nativeToJava();

  static {
    // 2.加载实现了native函数的动态库，只需要写动态库的名字
    System.load("/Users/tianchen/CLionProjects/arrow-jni/release/libtest_jni.dylib");
  }

  public static void main(String[] args) throws Exception {

    // java -> native
    VectorSchemaRoot root = generateVectorSchemaRoot();
    VectorUnloader unloader = new VectorUnloader(root);
    ArrowRecordBatch recordBatch = unloader.getRecordBatch();

    // serialize schema
    Schema schema = root.getSchema();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    byte[] schemaBytes = out.toByteArray();

    // extract buffers address
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();
    long[] bufAddrs = new long[buffers.size()];
    long[] bufSizes = new long[buffers.size()];

    int idx = 0;
    for (ArrowBuf buf : buffers) {
      bufAddrs[idx++] = buf.memoryAddress();
    }

    idx = 0;
    for (ArrowBuffer bufLayout : buffersLayout) {
      bufSizes[idx++] = bufLayout.getSize();
    }

    // call JNI
    javaToNative(schemaBytes, bufAddrs, bufSizes, root.getRowCount());




    // native -> java
    DataNode node = nativeToJava();
    Schema nativeSchema = readNativeSchema(node.schemas);

    List<FieldVector> vectors = new ArrayList<>();
    long[] nativeAddrs = node.bufAddrs;
    long[] nativeSizes = node.bufSizes;
    int index = 0;
    for (Field field : nativeSchema.getFields()) {
      FieldVector vector = field.createVector(allocator);
      List<ArrowBuf> fieldBuffers = new ArrayList<>();
      for (int i = 0; i < vector.getFieldBuffers().size(); i++) {
        NativeBufWrapper nativeBufWrapper = new NativeBufWrapper(nativeAddrs[index], nativeSizes[index]);
        ArrowBuf buf = new ArrowBuf(new NativeReferenceManager(nativeBufWrapper), null,
            (int) nativeBufWrapper.getSize(), nativeBufWrapper.getMemoryAddress(), false);
        fieldBuffers.add(buf);
        index++;
      }
      vector.loadFieldBuffers(new ArrowFieldNode(node.valueCount, 0), fieldBuffers);
      vectors.add(vector);
    }

    System.out.println("vectors from native:");
    for (FieldVector vector : vectors) {
      StringBuilder builder = new StringBuilder("[\n");
      for (int i = 0; i < vector.getValueCount(); i++) {
        builder.append("  " + vector.getObject(i) + "\n");
      }
      builder.append("]");
      System.out.println(builder.toString());
    }

  }

  private static Schema readNativeSchema(byte[] schemaBytes) throws Exception {
    try (MessageChannelReader schemaReader = new MessageChannelReader(new ReadChannel(new ByteArrayReadableSeekableByteChannel(schemaBytes)), allocator)) {

      MessageResult result = schemaReader.readNext();
      if (result == null) {
        throw new IOException("Unexpected end of input. Missing schema.");
      }

      return MessageSerializer.deserializeSchema(result.getMessage());
    }
  }

  private static VectorSchemaRoot generateVectorSchemaRoot() {

    IntVector vector1 = new IntVector("v1", allocator);
    vector1.allocateNewSafe();
    vector1.setSafe(0, 1);
    vector1.setSafe(1, 2);
    vector1.setSafe(2, 3);

    VarCharVector vector2 = new VarCharVector("v2", allocator);
    vector2.allocateNewSafe();
    vector2.setSafe(0, "aa".getBytes(StandardCharsets.UTF_8));
    vector2.setSafe(1, "bb".getBytes(StandardCharsets.UTF_8));
    vector2.setSafe(2, "cc".getBytes(StandardCharsets.UTF_8));

    VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(vector1.getField(), vector2.getField()),
        Arrays.asList((FieldVector) vector1, vector2), 3);
    return root;
  }
}
