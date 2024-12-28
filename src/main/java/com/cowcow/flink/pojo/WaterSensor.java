package com.cowcow.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {

    public String id;
    public Long ts;
    public Integer vc;


}
