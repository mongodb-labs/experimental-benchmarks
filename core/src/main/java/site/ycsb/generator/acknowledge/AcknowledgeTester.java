package site.ycsb.generator.acknowledge;

public class AcknowledgeTester {
    
    public static void main(String[] args) {
        System.out.println("starting");
        DefaultAcknowledgedCounterGenerator am = new DefaultAcknowledgedCounterGenerator(10500000000L);
        long l = am.lastValue();
        System.out.println("getting values");
        for(long i = 0; i < (1 << 40); i++) {
            am.nextValue();
        }
        System.out.println("acknowledging pt1");
        for(long i = 0; i < (1 << 10); i++) {
            am.acknowledge(l + i);
        }
        System.out.println("acknowledging pt2");
        for(long i = (1 << 30); i < (1 << 40); i++) {
            am.acknowledge(l + i);
        }
        System.out.println("acknowledging pt3");
        for(long i = (1 << 20); i < (1 << 30); i++) {
            am.acknowledge(l + i);
        }
        System.out.println("acknowledging pt4");
        for(long i = (1 << 10); i < (1 << 20); i++) {
            am.acknowledge(l + i);
        }
        System.out.println("done");
    }
}
