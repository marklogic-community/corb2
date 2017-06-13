package com.marklogic.developer.corb.util;

import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class CollectionUtils {
    private CollectionUtils(){}
    
    /**
     * Converts proeprties to a map of strings. 
     * @param properties
     * @return
     */
    public static Map<String,String> propertiesToMap(Properties properties){
        Map<String,String> map = new HashMap<String,String>();
        for (final String name: safe(properties).stringPropertyNames()){
            map.put(name, properties.getProperty(name));
        }
        return map;
    }
    
    /**
     * Removes entries with null keys or values and trims the value. 
     * @param inMap
     * @return
     */
    public static Map<String,String> removeNullsAndTrim(Map<String,String> inMap){
        Map<String,String> outMap = new HashMap<String,String>();
        for (final Map.Entry<String,String> entry: safe(inMap).entrySet()){
            if(entry.getKey() != null && entry.getValue() != null){
                outMap.put(entry.getKey().trim(), entry.getValue().trim());
            }
        }
        return outMap;
    }
    
    /**
     * Removes entries with null or blank keys and values and trims with balues. 
     * @param inMap
     * @return
     */
    public static Map<String,String> removeBlanksAndTrim(Map<String,String> inMap){
        Map<String,String> outMap = new HashMap<String,String>();
        for (final Map.Entry<String,String> entry: safe(inMap).entrySet()){
            if(isNotBlank(entry.getKey()) && isNotBlank(entry.getValue())){
                outMap.put(entry.getKey().trim(), entry.getValue().trim());
            }
        }
        return outMap;
    }
    
    public static <K, V> Map<K,V> safe(Map<K,V> map){
        return map != null ? map : Collections.emptyMap();
    }
    
    public static <T> List<T> safe(List<T> list){
        return list != null ? list : Collections.emptyList();
    }
    
    public static <T> Set<T> safe(Set<T> set){
        return set != null ? set : Collections.emptySet();
    }
    
    public static Properties safe(Properties props){
        return props != null ? props : new Properties();
    }

}
