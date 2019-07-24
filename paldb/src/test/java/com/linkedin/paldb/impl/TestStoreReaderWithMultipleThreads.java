package com.linkedin.paldb.impl;

import com.linkedin.paldb.api.Configuration;
import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.api.Serializer;
import com.linkedin.paldb.api.StoreReader;
import com.linkedin.paldb.api.StoreWriter;
import com.linkedin.paldb.utils.LongPacker;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.linkedin.paldb.api.Configuration.CACHE_BYTES;
import static com.linkedin.paldb.api.Configuration.MMAP_SEGMENT_SIZE;

public class TestStoreReaderWithMultipleThreads
{
    // test PalDB storage location
    private final File STORE_FOLDER = new File("data");
    private final File STORE_FILE = new File(STORE_FOLDER, "paldb.dat");

    // stores the generated test values to verify them after concurrent read from PalDB
    private Map<Integer, DoubleContainer> testValues;
    private final int NUMBER_OF_TEST_VALUES = 1000000;
    private final int ARRAY_LENGTH = 256;

    /**
     * Prepares a storage with 100,000 entries of double arrays with 256 values
     */
    @BeforeClass
    public void setup()
    {
        STORE_FILE.delete();
        STORE_FOLDER.delete();
        STORE_FOLDER.mkdir();

        Random random = new Random();
        testValues = new HashMap<Integer, DoubleContainer>();

        for (int i = 0; i < NUMBER_OF_TEST_VALUES; i++)
        {
            double[] valueArray = new double[ARRAY_LENGTH];
            for (int j = 0; j < ARRAY_LENGTH; j++) {
                valueArray[j] = random.nextDouble();
            }
            testValues.put(i, new DoubleContainer(valueArray));
        }

        Configuration config = new Configuration();
        config.set(Configuration.MMAP_DATA_ENABLED, String.valueOf(false));
        config.set(Configuration.CACHE_ENABLED, String.valueOf(true));
        //config.set(MMAP_SEGMENT_SIZE, "52428800");
        config.set(CACHE_BYTES, "104857600");
        config.registerSerializer(new DoubleContainerSerializer());
        StoreWriter writer = PalDB.createWriter(STORE_FILE, config);
        for (Map.Entry<Integer, DoubleContainer> entry : testValues.entrySet()) {
            Integer key = entry.getKey();
            DoubleContainer value = entry.getValue();
            writer.put(key, value);
        }
        writer.close();
    }

    /**
     * Clean up test storage location
     */
    @AfterClass
    public void cleanup()
    {
        STORE_FILE.delete();
        STORE_FOLDER.delete();
    }

    /**
     * Executes 20 threads which use the same StoreReader instance to read 100,000 values.
     *
     * @throws InterruptedException
     */
    @Test
    public void testThreadedRead() throws InterruptedException
    {
        final int threads = 10;
        final int keysToReadPerExecution = 100000;

        // configuration with 1mb mmap size and 25mb LRU cache
        Configuration readConfig = new Configuration();
        readConfig.set(Configuration.MMAP_DATA_ENABLED, String.valueOf(false));
        readConfig.set(Configuration.CACHE_ENABLED, String.valueOf(true));
        //readConfig.set(MMAP_SEGMENT_SIZE, "26214400");
        readConfig.set(CACHE_BYTES, "504857600");
        readConfig.registerSerializer(new DoubleContainerSerializer());
        StoreReader reader = PalDB.createReader(STORE_FILE, readConfig);

        // execute threads
        final CountDownLatch latch = new CountDownLatch(threads);
        final AtomicBoolean success = new AtomicBoolean(true);
        for (int i = 0; i < threads; i++)
        {
            new Thread(new ReaderTask( i, keysToReadPerExecution, reader, latch, success)).start();
        }
        latch.await();

        Assert.assertTrue(success.get());
    }

    private class ReaderTask implements Runnable
    {
        private final int threadId;
        private final int keysToRead;
        private final StoreReader reader;
        private final CountDownLatch latch;
        private final AtomicBoolean success;

        private final Random random = new Random();

        ReaderTask(int threadId, int keysToRead, StoreReader reader, CountDownLatch latch, AtomicBoolean success)
        {
            this.threadId = threadId;
            this.keysToRead = keysToRead;
            this.reader = reader;
            this.latch = latch;
            this.success = success;
        }

        /**
         * Reads random values from the given storage and verifies them with the previously generated test values
         */
        @Override
        public void run()
        {
            try
            {
                long start = System.currentTimeMillis();
                Integer[] keys = GenerateTestData.generateRandomIntKeys(1000000, NUMBER_OF_TEST_VALUES, (long) 4242);
                for ( int i = 0; i < keys.length; i++ )
                {
                    int key = keys[i];
                    DoubleContainer container = reader.get(key);
                    if ( !testValues.get(key).equals(container) )
                    {
                        throw new RuntimeException("fail");
                    }
                }
                long finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                System.out.println("Thread" + threadId + " finished in " + timeElapsed + "ms");
            }
            catch (Exception e)
            {
                e.printStackTrace();
                success.set(false);
            }
            finally
            {
                latch.countDown();
            }
        }
    }

    public class DoubleContainer
    {
        public double[] values;

        public DoubleContainer(double[] values)
        {
            this.values = values;
        }

        @Override
        public boolean equals(Object o)
        {
            return Arrays.equals(values, ((DoubleContainer)o).values);
        }
    }

    public class DoubleContainerSerializer implements Serializer<DoubleContainer>
    {
        private static final long serialVersionUID = 1L;

        @Override
        public void write(DataOutput dataOutput, DoubleContainer input) throws IOException
        {
            LongPacker.packInt(dataOutput, input.values.length);

            for (double d : input.values)
            {
                dataOutput.writeDouble(d);
            }
        }

        @Override
        public DoubleContainer read(DataInput dataInput) throws IOException
        {
            int length = LongPacker.unpackInt(dataInput);

            double[] data = new double[length];
            for (int i = 0; i < length; i++)
            {
                data[i] = dataInput.readDouble();
            }

            return new DoubleContainer(data);
        }

        @Override
        public int getWeight(DoubleContainer instance)
        {
            return instance != null
                    ? 8 * instance.values.length
                    : 0;
        }
    }
}