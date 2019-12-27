import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;

public class Test01 {
    public static void main(String[] args) {
        /*String str ="{\"data\":[{\"ID\":\"1\",\"code\":\"C0256\",\"name\":\"0元天马二手车123\",\"kind\":\"1\",\"fax\":\"\",\"address\":\"雨花区宁双路18号沁恒产业园A座201\",\"region\":\"2072\",\"margins\":\"2000\",\"aucPrLevel\":\"1\",\"status\":\"2\",\"frozenType\":\"1\",\"frozenTime\":\"2019-05-11 14:36:51\",\"userName\":\"7c6e844445948ed9che++\",\"password\":\"B46A559E07181367B1A53C52A62966D1\",\"latestLoginTime\":null,\"unFreezeTime\":\"2018-12-11 09:50:02\",\"jbr\":null,\"jbrName\":null,\"fzr\":\"1979\",\"fzrName\":\"杨丽萍\",\"bak\":\"测试账号\",\"level\":\"2\",\"addTime\":\"2013-03-07\",\"phone\":null,\"contractNo\":null,\"contractExpTime\":null,\"cutime\":\"2019-11-25 11:20:24\",\"contractImg\":null,\"contractImg_dfsId\":null,\"contractImg_ossId\":\"0\",\"barginCount\":\"0\",\"buyerGrade\":\"铜牌\",\"openTime\":\"2013-03-09\",\"closeTime\":null,\"buyerIsBid\":\"1\",\"isApp\":\"0\",\"channelSource\":\"0\",\"isId5\":\"0\",\"id5CheckRecordId\":null,\"memo\":\"\",\"boothNo\":null,\"marginsRule\":\"1\",\"userType\":\"0\",\"userNameModNum\":\"0\",\"oldFlag\":\"0\",\"openAccount\":\"张桃桃\",\"openAccountId\":\"5442\",\"ucFlag\":\"1\",\"mainBuyerId\":\"-1\"}],\"database\":\"chezhibao\",\"es\":1574652024000,\"id\":3,\"isDdl\":false,\"mysqlType\":{\"ID\":\"int(11)\",\"code\":\"varchar(30)\",\"name\":\"varchar(30)\",\"kind\":\"tinyint(4)\",\"fax\":\"varchar(20)\",\"address\":\"varchar(200)\",\"region\":\"int(11)\",\"margins\":\"int(11)\",\"aucPrLevel\":\"tinyint(4)\",\"status\":\"tinyint(4)\",\"frozenType\":\"tinyint(4)\",\"frozenTime\":\"datetime\",\"userName\":\"varchar(255)\",\"password\":\"varchar(32)\",\"latestLoginTime\":\"datetime\",\"unFreezeTime\":\"datetime\",\"jbr\":\"int(11)\",\"jbrName\":\"varchar(30)\",\"fzr\":\"int(11)\",\"fzrName\":\"varchar(30)\",\"bak\":\"varchar(300)\",\"level\":\"tinyint(4)\",\"addTime\":\"date\",\"phone\":\"varchar(15)\",\"contractNo\":\"varchar(50)\",\"contractExpTime\":\"datetime\",\"cutime\":\"timestamp\",\"contractImg\":\"varchar(300)\",\"contractImg_dfsId\":\"varchar(30)\",\"contractImg_ossId\":\"varchar(50)\",\"barginCount\":\"int(11)\",\"buyerGrade\":\"varchar(30)\",\"openTime\":\"date\",\"closeTime\":\"date\",\"buyerIsBid\":\"int(11)\",\"isApp\":\"tinyint(4)\",\"channelSource\":\"tinyint(4)\",\"isId5\":\"int(11)\",\"id5CheckRecordId\":\"int(11)\",\"memo\":\"varchar(300)\",\"boothNo\":\"varchar(100)\",\"marginsRule\":\"int(11)\",\"userType\":\"tinyint(4)\",\"userNameModNum\":\"tinyint(4)\",\"oldFlag\":\"tinyint(2)\",\"openAccount\":\"varchar(30)\",\"openAccountId\":\"int(11)\",\"ucFlag\":\"tinyint(4)\",\"mainBuyerId\":\"int(11)\"},\"old\":[{\"name\":\"0元天马二手车12\",\"cutime\":\"2019-11-22 11:14:35\"}],\"pkNames\":[\"ID\"],\"sql\":\"\",\"sqlType\":{\"ID\":4,\"code\":12,\"name\":12,\"kind\":-6,\"fax\":12,\"address\":12,\"region\":4,\"margins\":4,\"aucPrLevel\":-6,\"status\":-6,\"frozenType\":-6,\"frozenTime\":93,\"userName\":12,\"password\":12,\"latestLoginTime\":93,\"unFreezeTime\":93,\"jbr\":4,\"jbrName\":12,\"fzr\":4,\"fzrName\":12,\"bak\":12,\"level\":-6,\"addTime\":91,\"phone\":12,\"contractNo\":12,\"contractExpTime\":93,\"cutime\":93,\"contractImg\":12,\"contractImg_dfsId\":12,\"contractImg_ossId\":12,\"barginCount\":4,\"buyerGrade\":12,\"openTime\":91,\"closeTime\":91,\"buyerIsBid\":4,\"isApp\":-6,\"channelSource\":-6,\"isId5\":4,\"id5CheckRecordId\":4,\"memo\":12,\"boothNo\":12,\"marginsRule\":4,\"userType\":-6,\"userNameModNum\":-6,\"oldFlag\":-6,\"openAccount\":12,\"openAccountId\":4,\"ucFlag\":-6,\"mainBuyerId\":4},\"table\":\"t_buyer\",\"ts\":1574652024107,\"type\":\"UPDATE\"}\n";
        String str1 ="(CHANNELSOURCE,CONTRACTIMG_OSSID,STATUS,CONTRACTIMG,CUTIME,BD_TIMESTAMP,USERNAME,JBR,MEMO,ID5CHECKRECORDID,MARGINSRULE,UCFLAG,BARGINCOUNT,MARGINS,ADDTIME,LEVEL,NAME,REGION,OPENACCOUNT,USERNAMEMODNUM,PASSWORD,KIND,OPENTIME,ADDRESS,FZRNAME,ISID5,ISAPP,CLOSETIME,FROZENTYPE,PHONE,CONTRACTIMG_DFSID,CONTRACTNO,UNFREEZETIME,AUCPRLEVEL,BD_TYPE,LATESTLOGINTIME,USERTYPE,BUYERISBID,OLDFLAG,MAINBUYERID,BAK,JBRNAME,FROZENTIME,CODE,CONTRACTEXPTIME,OPENACCOUNTID,FAX,BOOTHNO,BUYERGRADE,FZR) ";
        int length = str1.split(",").length;
        System.out.println(length);
        JSONObject jsonObject2 = JSON.parseObject(str1);

        JSONObject jsonObject = JSON.parseObject(str);
        //JSONObject pkNames = jsonObject.getJSONArray("pkNames").getJSONObject(0);

        System.out.println(jsonObject2.getString("pkNames").replace("\"","").replace("[","").replace("]",""));
        //boolean gg = jsonObject.getString("gg").isEmpty();
        //System.out.println(gg);
        JSONArray data = jsonObject.getJSONArray("data");
        JSONObject jsonObject1 = data.getJSONObject(0);
        String id = jsonObject1.getString("ee");
        System.out.println(String.valueOf(System.currentTimeMillis()));*/

       /* Set<String> strings = jsonObject1.keySet();
        Iterator<String> iterator = strings.iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            String value = jsonObject1.getString(key);
            //Byte aByte = jsonObject1.getByte(key);
            //System.out.println(key + ":"+ aByte);
            System.out.println(key + ":"+ value);
        }*/
       // System.out.println(jsonObject1.getString("ID"));
         /*String driverName = "org.apache.hive.jdbc.HiveDriver";
         String url = "jdbc:hive2://escnode1:10000/test";
         String user = "hadoop";
         String password = "";

         Connection conn = null;
         Statement stmt = null;
         ResultSet rs = null;*/

        /*String st = "2000,2012-12-09,2019-09-09 15:12:50,null,null,0,0,74B44A6E2721E56B4556F0F486AD654B,10,陈勇,null,null,17917,陈煜3,0,1,6241,2,0,2019-12-11 18:53:44,铜牌,2019-05-11 14:36:51,null,eq,2019-09-09 15:13:04,null,0,2072,2,null,C0253833,1,null,null,null,0,-1,null,2014-06-27,1,2012-12-11,0,南京市花神大道23号,1,2018-12-11 09:50:02,0,6e6af381b82aebe6c3c2deb9d59a9776che++,0,1,10,UPDATE,1576061625438";
        System.out.println(st.split(",").length);*/
       /* String str = "varchar(200)";
        String reg = "[a-zA-Z]+";
        Pattern p = Pattern.compile(reg);
        Matcher matcher = p.matcher(str);
        matcher.find();
        System.out.print(matcher.group(0));*/


    }
}
