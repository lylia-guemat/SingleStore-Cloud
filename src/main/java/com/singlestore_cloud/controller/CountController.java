package com.singlestore_cloud.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class CountController {

    @Autowired
    private JdbcTemplate jdbcTemplate ;

    @GetMapping("/count")
    public Long getRowCount() {
        String sql = "Select COUNT(*) FROM `yellow_tripdata_2009-01`";
        return jdbcTemplate.queryForObject(sql, Long.class);
    }

    @GetMapping("/allTablesCount")
    public Map<String, Long> getAllTablesCount(){
        Map<String, Long> results = new HashMap<>();

        //recuperer la liste des tables
        List<String> tables = jdbcTemplate.queryForList("SHOW TABLES;",String.class) ;

        //pour chaque table, on compte le nombre de ligne (pas trop efficace en terme de performance car si on a bcp de tables ca peut etre long, utiliser information_shema
        for(String table : tables) {
            String sql = "SELECT COUNT(*) FROM `"+table+"`" ;
            Long count = jdbcTemplate.queryForObject(sql, Long.class);
            results.put(table, count) ;
        }
        return results;
    }

    @GetMapping("/show")
    public List<Map<String, Object>> ShowRows() {
        String sql = "Select * FROM `yellow_tripdata_2009-01` Limit 10" ;
        return jdbcTemplate.queryForList(sql);
    }
}
