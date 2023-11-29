package cn.com.shinano.nameserver.util;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.nameserver.config.NameServerConfig;
import cn.com.shinano.nameserver.dto.RegisteredHost;
import com.alibaba.fastjson.JSON;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
public class FileUtil {
    public static String clientId;

    public static ConcurrentMap<String, Set<RegisteredHost>> loadRegistryInfoFromDisk() throws IOException {
        ConcurrentMap<String, Set<RegisteredHost>> map = new ConcurrentHashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(registryInfoDiskPath().toFile()))){
            String line = null;
            while ((line=br.readLine()) != null) {
                String[] split = line.split("@~");
                List<RegisteredHost> hosts = JSON.parseArray(split[1], RegisteredHost.class);
                HashSet<RegisteredHost> set = new HashSet<>(hosts);
                map.put(split[0], set);
            }
        }
        return map;
    }

    public static void saveRegistryInfoToDisk(ConcurrentMap<String, Set<RegisteredHost>> registeredService) throws IOException {

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(registryInfoDiskPath().toFile()))){
            StringBuilder sb = new StringBuilder();
            for (String serviceId : registeredService.keySet()) {
                sb.append(serviceId).append("@~");
                registeredService.computeIfPresent(serviceId, (k,v)->{
                    sb.append(JSON.toJSONString(v));
                    return v;
                });
                sb.append('\n');
            }
            bw.write(sb.toString());
        }
    }

    public static Path registryInfoDiskPath() throws IOException {
        Path path = Paths.get(String.format(NameServerConfig.REGISTRY_INFO_DISK_PATH, clientId));
        if(!Files.exists(path)) {
            Files.createDirectories(path.getParent());
            Files.createFile(path);
        }
        return path;
    }
}
