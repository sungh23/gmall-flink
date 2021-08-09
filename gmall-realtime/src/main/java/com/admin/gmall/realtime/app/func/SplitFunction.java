package com.admin.gmall.realtime.app.func;

import com.admin.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

  public void eval(String str) {
    for (String s : KeywordUtil.splitKeyWord(str,true)) {
      // use collect(...) to emit a row
      collect(Row.of(s));
    }
  }
}