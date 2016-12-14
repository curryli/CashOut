import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
 
public class TimeUtil
{
	public static void main(String[] args)
    {
		System.out.println(DayofWeek("20161209"));
    }   
	
	
	
	public static int DayofWeek(String pdate){
		int year = Integer.parseInt(pdate.substring(0,4));
        int month = Integer.parseInt(pdate.substring(4,6));
        int day = Integer.parseInt(pdate.substring(6,8));
        Calendar calendar = Calendar.getInstance();//获得一个日历
        calendar.set(year, month-1, day);//设置当前时间,月份是从0月开始计算
        int number = calendar.get(Calendar.DAY_OF_WEEK);//星期表示1-7，是从星期日开始，   
        int [] str = {0,7,1,2,3,4,5,6};
        
        return str[number];
	}
	//int year = 2016;
	//int month = 12;
	//int day = 9;
	//Calendar calendar = Calendar.getInstance();//获得一个日历
	//calendar.set(year, month-1, day);//设置当前时间,月份是从0月开始计算
	//int number = calendar.get(Calendar.DAY_OF_WEEK);//星期表示1-7，是从星期日开始，   
	//String [] str = {"","星期日","星期一","星期二","星期三","星期四","星期五","星期六",};
	//System.out.println(str[number]);
	
	
	public static Double getDeltaMin(String time1, String time2){
		Date date1;	
		Date date2;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			date1 = sdf.parse(time1);
			date2 = sdf.parse(time2);
			return (double) Math.abs((date1.getTime()-date2.getTime()))/(1000*60);//1000*60*60*24
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}

