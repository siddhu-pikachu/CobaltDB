package dbms;


public class Condition {
    String column_name;
    private OperandType operator;
    String comparator_value;
    boolean negation;
    public int column_ordinal;
    public DataType data_type;

    public Condition(DataType data_type) {
        this.data_type = data_type;
    }

    public static String[] supportedOperators = { "<=", ">=", "<>", ">", "<", "=" };

    public static OperandType get_typeOf_operator(String strOperator) {
        switch (strOperator) {
        case ">":
            return OperandType.GREATERTHAN;
        case "<":
            return OperandType.LESSTHAN;
        case "=":
            return OperandType.EQUALTO;
        case ">=":
            return OperandType.GREATERTHANOREQUAL;
        case "<=":
            return OperandType.LESSTHANOREQUAL;
        case "<>":
            return OperandType.NOTEQUAL;
        default:
            System.out.println("! Invalid operator \"" + strOperator + "\"");
            return OperandType.INVALID;
        }
    }

    public static int compare(String value1, String value2, DataType data_type) {
        if (data_type == DataType.TEXT)
            return value1.toLowerCase().compareTo(value2);
        else if (data_type == DataType.NULL) {
            if (value1 == value2)
                return 0;
            else if (value1.toLowerCase().equals("null"))
                return 1;
            else
                return -1;
        } else {
            return Long.valueOf(Long.parseLong(value1) - Long.parseLong(value2)).intValue();
        }
    }

    public boolean condition_check(String currentValue) {
        OperandType operation = getOperation();
        if(currentValue.toLowerCase().equals("null")
        || comparator_value.toLowerCase().equals("null"))
            return on_difference_operation(operation,compare(currentValue,comparator_value,DataType.NULL));
        if (data_type == DataType.TEXT || data_type == DataType.NULL)
            return compare_string(currentValue, operation);
        else {
            switch (operation) {
            case LESSTHANOREQUAL:
                return Long.parseLong(currentValue) <= Long.parseLong(comparator_value);
            case GREATERTHANOREQUAL:
                return Long.parseLong(currentValue) >= Long.parseLong(comparator_value);

            case NOTEQUAL:
                return Long.parseLong(currentValue) != Long.parseLong(comparator_value);
            case LESSTHAN:
                return Long.parseLong(currentValue) < Long.parseLong(comparator_value);

            case GREATERTHAN:
                return Long.parseLong(currentValue) > Long.parseLong(comparator_value);
            case EQUALTO:
                return Long.parseLong(currentValue) == Long.parseLong(comparator_value);

            default:
                return false;
            }
        }
    }

    public void setConditionValue(String conditionValue) {
        this.comparator_value = conditionValue;
        this.comparator_value = comparator_value.replace("'", "");
        this.comparator_value = comparator_value.replace("\"", "");

    }

    public void setColumName(String columnName) {
        this.column_name = columnName;
    }

    public void setOperator(String operator) {
        this.operator = get_typeOf_operator(operator);
    }

    public void setNegation(boolean negate) {
        this.negation = negate;
    }

    public OperandType getOperation() {
        if (!negation)
            return this.operator;
        else
            return negateOperator();
    }

    private boolean on_difference_operation(OperandType operation,int difference)
    {
        switch (operation) {
            case LESSTHANOREQUAL:
            return difference <= 0;
        case GREATERTHANOREQUAL:
            return difference >= 0;
        case NOTEQUAL:
            return difference != 0;
        case LESSTHAN:
            return difference < 0;
        case GREATERTHAN:
            return difference > 0;
        case EQUALTO:
            return difference == 0;
        default:
            return false;
        }
    }

    private boolean compare_string(String currentValue, OperandType operation) {
        return on_difference_operation(operation,currentValue.toLowerCase().compareTo(comparator_value));
    }    

    private OperandType negateOperator() {
        switch (this.operator) {
        case LESSTHANOREQUAL:
            return OperandType.GREATERTHAN;
        case GREATERTHANOREQUAL:
            return OperandType.LESSTHAN;
        case NOTEQUAL:
            return OperandType.EQUALTO;
        case LESSTHAN:
            return OperandType.GREATERTHANOREQUAL;
        case GREATERTHAN:
            return OperandType.LESSTHANOREQUAL;
        case EQUALTO:
            return OperandType.NOTEQUAL;
        default:
            System.out.println("! Invalid operator \"" + this.operator + "\"");
            return OperandType.INVALID;
        }
    }
}

