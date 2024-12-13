package dbms;

import java.util.HashMap;
import java.util.Map;

public enum PageType {
    INTERIORINDEX((byte)2),
    LEAF((byte)13),
    INTERIOR((byte)5),
    LEAFINDEX((byte)10);
     
 private static final Map<Byte,PageType> pageTypeLookup = new HashMap<Byte,PageType>();
 private byte value;

 static {
      for(PageType pgtyp : PageType.values())
      pageTypeLookup.put(pgtyp.getValue(), pgtyp);
 }
 
 public byte getValue() { return value; }

 public static PageType get(byte value) { 
      return pageTypeLookup.get(value); 
 }

 private PageType(byte value) {
      this.value = value;
 }

 
}