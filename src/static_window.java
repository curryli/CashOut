import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

 


public class static_window {
	
	/**
     * @Title: calMaxIn7
     * @Description: 计算连续七天最多的交易的次数
     * @param startDate
     * @param endDate
     * @param map key:日期字符串; value:交易次数
     * @return
     * @return: long
     */
    public static long calMaxIn7(String startDate, String endDate, Map<String, Long> map) {    
    	//这里如果没有事先二次排序的话    由于reducer是根据卡号作为key的，所以在每个reducer里面针对卡号维护一个map  key是日期，value是金额    
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance();
		Calendar calendar2 = Calendar.getInstance();
		String tempDate, tempDate2;
		
		try {
			calendar.setTime(simpleDateFormat.parse(startDate));
			calendar2.setTime(simpleDateFormat.parse(startDate));
			
			long max = 0, temp = 0;
			
			for (int i = 0; i < 7; i ++) {
				tempDate = simpleDateFormat.format(calendar.getTime());
				if (map.containsKey(tempDate)) {
					max += map.get(tempDate);
				}
				calendar.add(Calendar.DAY_OF_MONTH, 1);
			}
			temp = max;
			for (; calendar.getTime().compareTo(simpleDateFormat.parse(endDate)) <= 0; calendar.add(Calendar.DAY_OF_MONTH, 1)) {
				tempDate = simpleDateFormat.format(calendar.getTime());
				tempDate2 = simpleDateFormat.format(calendar2.getTime());
				if (map.containsKey(tempDate)) {
					temp += map.get(tempDate);
				}
				if (map.containsKey(tempDate2)) {
					temp -= map.get(tempDate2);
				}
				max = max > temp ? max : temp;
				calendar.add(Calendar.DAY_OF_MONTH, 1);
				calendar2.add(Calendar.DAY_OF_MONTH, 1);
			}
			
			return max;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
		
	}


    public static Double calTotal(TreeMap<String, Double> qmoney) {
    	//如果已经二次排序好的话  由于reducer仍然是根据卡号作为key的，所以在每个reducer里面维护一个list，其中每个元素是(时间，金额)。 如果key时间比当前日期大于7天，那么就将该元素从集合中移除，然后计算现在这个list的总金额或者最大值或者平均值即可    
    	Double periodMSum = 0.0;
    	Iterator<String> ir = qmoney.keySet().iterator();
    	
    	
    	while(ir.hasNext())
        {
           // System.out.println(Qmoney.iterator().next());
    		periodMSum = periodMSum + qmoney.get(ir.next());
        }
    	
    	return periodMSum;
    }
}
