package cloud.agileframework.sql;

import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author 佟盟
 * 日期 2020-10-14 16:00
 * 描述 TODO
 * @version 1.0
 * @since 1.0
 */
public class WhereIn {
    private final String oldSql;
    private final List<Object> in;

    public WhereIn(String oldSql, List<Object> in) {
        this.oldSql = oldSql;
        this.in = in;
    }

    public String sql() {
        return IntStream.range(0, in.size()).mapToObj(i -> oldSql + i).collect(Collectors.joining(","));
    }
}
