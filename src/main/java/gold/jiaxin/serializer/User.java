package gold.jiaxin.serializer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * TODO
 *
 * @author yanxiaodong
 * @version 1.0
 * @date 2020/11/16 10:30
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User implements Serializable {

    private static final long serialVersionUID = -6062177989498703792L;

    private Integer id;
    private String name;
    private Date birthDay;

}
