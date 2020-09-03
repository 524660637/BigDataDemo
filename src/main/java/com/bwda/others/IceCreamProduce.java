package com.bwda.others;
//
//题目：设计生产系统
//
//        有一家生产冰激淋的厂家，每天需要生产100000份冰激淋卖给超市，通过一辆送货车发货，送货车辆每次送100份。
//        厂家有一个容量为1000份的冷库，用于保鲜，生产的冰激淋需要先存放在冷库，运输车辆从冷库取货。
//        厂家有三条生产线，分别是牛奶供应生产线，发酵剂制作生产线，冰激淋生产线。生产每份冰激淋需要2份牛奶和1份发酵剂。
//        请设计生产系统，给出必要的代码和文档。

import java.util.concurrent.*;

public class IceCreamProduce {
    private static final Integer MILKMATERIALNUM=200000;
    private static final Integer FAJIAOMATERIALNUM=100000;
    private static final Integer WAREHOUSECAPICITY=1000;
    private final static BlockingQueue<Integer> warehouseQueue = new ArrayBlockingQueue<Integer>(WAREHOUSECAPICITY);
    private final static BlockingQueue<Integer> milkQueue = new ArrayBlockingQueue<Integer>(MILKMATERIALNUM);
    private final static BlockingQueue<Integer> fajiaoQueue = new ArrayBlockingQueue<Integer>(FAJIAOMATERIALNUM);
    private static ExecutorService executor = Executors.newFixedThreadPool(2);
    private static Integer ICECREAMNUM = 0;


    public static void main(String args[]){
        //材料准备线程,非阻塞
    new Thread(new preapareMilkMaterial(MILKMATERIALNUM)).start();
    new Thread(new preapareFajiaoMaterial(FAJIAOMATERIALNUM)).start();

    while(true){
        productIceaCream();
    }
    }

    public static void consumerCar(Integer capacity){
        try {
            int i=0;
            while (true) {
                warehouseQueue.take();
                i++;
                if(i==capacity)break;
            }
        } catch (InterruptedException e) {
            System.err.println("冷库没库存了！");
            Thread.currentThread().interrupt();
        }
        System.out.printf("成功运出牛奶%d份！\n",capacity);
    }

    public static void productIceaCream(){
        //生产线程带返回结果,阻塞
        Future<Boolean> futureMilk = executor.submit(new productMilk());
        Future<Boolean> futureFaJiao = executor.submit(new productFaJiao());
        try {
            if(futureMilk.get()&&futureFaJiao.get()){
                System.out.println("已成功生产出冰淇淋1份！");
            }
        } catch (InterruptedException e) {
//            System.err.println("冰淇淋还未生产成功！");
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            System.err.println("冰淇淋生产失败！");
        }
        try {
//            warehouseQueue.put(1);
            warehouseQueue.add(1);
            ICECREAMNUM++;
            if(ICECREAMNUM%100 ==0){
                consumerCar(100);
            }
            System.out.printf("冰淇淋成功入库一份！库存%d\n",warehouseQueue.size());
        } catch (Exception e) {
            System.err.println("冷库已满，冰淇淋入库失败！等待十秒后继续生产");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            e.printStackTrace();
        }
    }

   static class productMilk implements Callable<Boolean> {
        public Boolean call() {
            try {
                int i=0;
                while (true) {
                    milkQueue.take();
                    i++;
                    if(i>1)break;
                }
            } catch (InterruptedException e) {
//                System.err.println("牛奶没了！");
                Thread.currentThread().interrupt();
                return false;
            }
            System.out.println("成功获取牛奶2份，剩余"+milkQueue.size());
            return true;
        }
    }
    static class productFaJiao implements Callable<Boolean> {
        public Boolean call() {
            try {
                int i=0;
                while (true) {
                    fajiaoQueue.take();
                    i++;
                    if(i>0)break;
                }
            } catch (InterruptedException e) {
//                System.err.println("发酵剂没了！");
                Thread.currentThread().interrupt();
                return false;
            }
            System.out.println("成功获取发酵剂1份，剩余"+fajiaoQueue.size());
            return true;
        }

    }

    static class preapareMilkMaterial implements Runnable {
        Integer milkCapcity;
        public preapareMilkMaterial(Integer milkCapcity) {
            this.milkCapcity=milkCapcity;
        }
        public void run() {
            try {
                int milk=1;
                while (true){
                    milkQueue.put(1);
                    milk++;
                    if(milk>milkCapcity)break;
                }
            } catch (InterruptedException e) {
//                System.err.println("牛奶材料已满");
                Thread.currentThread().interrupt();
            }
        }
    }

    static class preapareFajiaoMaterial implements Runnable {
        Integer fajiaoCapcity;
        public preapareFajiaoMaterial(Integer fajiaoCapcity) {
            this.fajiaoCapcity=fajiaoCapcity;
        }
        public void run() {
            try {
                int fajiao=1;
                while (true){
                    fajiaoQueue.put(1);
                    fajiao++;
                    if(fajiao>fajiaoCapcity)break;
                }
            } catch (InterruptedException e) {
//                System.err.println("发酵剂材料已满");
                Thread.currentThread().interrupt();
            }
        }
    }
}
