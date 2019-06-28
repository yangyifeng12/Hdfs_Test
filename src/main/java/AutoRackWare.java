import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.*;

public class AutoRackWare implements DNSToSwitchMapping {

    @Override
    public List<String> resolve(List<String> hosts) {
        List<String> racks = new ArrayList<>();
        for(String host : hosts){
            int hostnum = 0;
            if(host.startsWith("node")){
                String hno = host.substring(4);
                hostnum = Integer.parseInt(hno);
            }else{
                String hno = host.substring(host.lastIndexOf(".")+1);
                hostnum = Integer.parseInt(hno);
            }
            if(hostnum<=153){
                racks.add("/rack1/" + hostnum);
            }else{
                racks.add("/rack2/" + hostnum);
            }

        }
        return racks;
    }

    @Override
    public void reloadCachedMappings() {
        // TODO Auto-generated method stub

    }

    @Override
    public void reloadCachedMappings(List<String> arg0) {
        // TODO Auto-generated method stub

    }

}