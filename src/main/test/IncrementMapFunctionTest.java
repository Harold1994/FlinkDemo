import flink.streaming.*;
import org.junit.Test;
import static org.junit.Assert.*;


public class IncrementMapFunctionTest {
    @Test
    public void map() throws Exception {
        IncrementMapFunction func = new IncrementMapFunction();
        assertEquals(Long.valueOf(3), func.map(2L));
    }
}