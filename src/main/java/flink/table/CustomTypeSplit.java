package flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class CustomTypeSplit extends TableFunction<Row> {
    String sep = " ";

    public CustomTypeSplit(String sep) {
        this.sep = sep;
    }

    public void eval(String str) {
        for(String s : str.split(sep)) {
            Row row = new Row(2);
            row.setField(0, s);
            row.setField(1, s.length());
            collect(row);
        }
    }

    @Override
    public TypeInformation getResultType() {
        return Types.ROW(Types.STRING(), Types.INT());
    }
}
