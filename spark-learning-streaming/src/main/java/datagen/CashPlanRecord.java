package datagen;

/**
 * @programe: cashstore
 * @description: CashPlanRecord
 * @author: lpq
 * @create: 2019-07-17 18:00
 **/
public class CashPlanRecord {
    private int id;
    private String machineNo;
    private String termPaymentDay;
    private String currency;
    private String branchNo;
    private String tellerID;
    private String operTime;
    private int cashNum;
    private int faceValue;

    public CashPlanRecord(int id, String machineNo, String termPaymentDay, String currency, String branchNo, String tellerID, String operTime, int cashNum, int faceValue) {
        this.id = id;
        this.machineNo = machineNo;
        this.termPaymentDay = termPaymentDay;
        this.currency = currency;
        this.branchNo = branchNo;
        this.tellerID = tellerID;
        this.operTime = operTime;
        this.cashNum = cashNum;
        this.faceValue = faceValue;
    }

    @Override
    public String toString() {
        return id+
                "," + machineNo +
                "," + termPaymentDay +
                ", " + currency +
                ", " + branchNo +
                ", " + tellerID +
                ", " + operTime +
                ", " + cashNum +
                ", " + faceValue;
    }
}
