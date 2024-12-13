package dbms;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.nio.charset.StandardCharsets;


public class Attribute
{
    //represents the byte array, the format stored in binary file
    public byte[] FieldValueByte;
    public Byte[] field_value_Byte;

    public DataType data_type;
    
    //converted string value of the attribute
    public String field_value;

    //constructor
    @SuppressWarnings("deprecation")
	Attribute(DataType dataType,byte[] fieldValue){
        this.data_type = dataType;
        this.FieldValueByte = fieldValue;
    try{
    //Convert the byte array into string
      switch(dataType)
      {
         case NULL:
            this.field_value= "NULL"; break;
        case TINYINT: this.field_value = Byte.valueOf(BytesConversion.byte_from_Byte_Array(FieldValueByte)).toString(); break;
        case SMALLINT: this.field_value = Short.valueOf(BytesConversion.short_from_Byte_Array(FieldValueByte)).toString(); break;
        case INT: this.field_value = Integer.valueOf(BytesConversion.int_from_Byte_Array(FieldValueByte)).toString(); break;
        case BIGINT: this.field_value =  Long.valueOf(BytesConversion.long_from_Byte_Array(FieldValueByte)).toString(); break;
        case FLOAT: this.field_value = Float.valueOf(BytesConversion.float_from_Byte_Array(FieldValueByte)).toString(); break;
        case DOUBLE: this.field_value = Double.valueOf(BytesConversion.double_from_Byte_Array(FieldValueByte)).toString(); break;
        case YEAR: this.field_value = Integer.valueOf((int)Byte.valueOf(BytesConversion.byte_from_Byte_Array(FieldValueByte))+2000).toString(); break;
        case TIME:
            int millisec_after_midnight = BytesConversion.int_from_Byte_Array(FieldValueByte) % 86400000;
            int seconds = millisec_after_midnight / 1000;
            int hours = seconds / 3600;
            int rem_hour_seconds = seconds % 3600;
            int minutes = rem_hour_seconds / 60;
            int remSeconds = rem_hour_seconds % 60;
            this.field_value = String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", remSeconds);
            break;
        case DATETIME:
            Date raw_date_time = new Date(Long.valueOf(BytesConversion.long_from_Byte_Array(FieldValueByte)));
            this.field_value = String.format("%02d", raw_date_time.getYear()+1900) + "-" + String.format("%02d", raw_date_time.getMonth()+1)
                + "-" + String.format("%02d", raw_date_time.getDate()) + "_" + String.format("%02d", raw_date_time.getHours()) + ":"
                + String.format("%02d", raw_date_time.getMinutes()) + ":" + String.format("%02d", raw_date_time.getSeconds());
            break;
        case DATE:
            // YYYY-MM-DD
            Date rawdate = new Date(Long.valueOf(BytesConversion.long_from_Byte_Array(FieldValueByte)));
            this.field_value = String.format("%02d", rawdate.getYear()+1900) + "-" + String.format("%02d", rawdate.getMonth()+1)
                + "-" + String.format("%02d", rawdate.getDate());
            break;
        case TEXT: this.field_value = new String(FieldValueByte, "UTF-8"); break;
         default:
         this.field_value= new String(FieldValueByte, "UTF-8"); break;
      }
         this.field_value_Byte = BytesConversion.byte_to_Bytes(FieldValueByte);
    } catch(Exception ex) {
        System.out.println("Incorrect Format!!");
    }

    }

    Attribute(DataType dataType,String fieldValue) throws Exception {
        this.data_type = dataType;
        this.field_value = fieldValue;

        //Convert the string value into byte array based on DataType

        try {
            switch(dataType)
            {
              case NULL:
                  this.FieldValueByte = null; break;
              case YEAR: this.FieldValueByte = new byte[] { (byte) (Integer.parseInt(fieldValue) - 2000) }; break;
              case TIME: this.FieldValueByte = BytesConversion.int_to_bytes(Integer.parseInt(fieldValue)); break;
              case DATETIME:
                  SimpleDateFormat sdftime = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                  Date datetime = sdftime.parse(fieldValue);  
                  this.FieldValueByte = BytesConversion.long_to_bytes(datetime.getTime());              
                break;
              case DATE:
                  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                  Date date = sdf.parse(fieldValue);  
                  this.FieldValueByte = BytesConversion.long_to_bytes(date.getTime());              
                break;
              case TEXT: this.FieldValueByte = fieldValue.getBytes(); break;
              case TINYINT: this.FieldValueByte = new byte[]{ Byte.parseByte(fieldValue)}; break;
              case SMALLINT: this.FieldValueByte = BytesConversion.short_to_bytes(Short.parseShort(fieldValue)); break;
              case INT: this.FieldValueByte = BytesConversion.int_to_bytes(Integer.parseInt(fieldValue)); break;
              case BIGINT: this.FieldValueByte =  BytesConversion.long_to_bytes(Long.parseLong(fieldValue)); break;
              case FLOAT: this.FieldValueByte = BytesConversion.float_to_bytes(Float.parseFloat(fieldValue)); break;
              case DOUBLE: this.FieldValueByte = BytesConversion.doubl_to_bytes(Double.parseDouble(fieldValue)); break;
               default:
               this.FieldValueByte = fieldValue.getBytes(StandardCharsets.US_ASCII); break;
            }
            this.field_value_Byte = BytesConversion.byte_to_Bytes(FieldValueByte);  
        } catch (Exception e) {
            System.out.println("Conversion " + fieldValue + " to " + dataType.toString()+" not permitted.");
            throw e;
        }
    }
   
}
