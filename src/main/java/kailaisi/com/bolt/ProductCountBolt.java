package kailaisi.com.bolt;

import com.alibaba.fastjson.JSONArray;
import kailaisi.com.zk.ZooKeeperSession;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

/**
 * 商品访问次数统计bolt
 *
 * @author Administrator
 */
public class ProductCountBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProductCountBolt.class);
    private static final long serialVersionUID = -8761807561458126413L;

    private LRUMap<Long, Long> productCountMap = new LRUMap<Long, Long>(1000);
    private ZooKeeperSession zkSession;
    private int taskId;

    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        new Thread(new ProductCountThread()).start();

        zkSession = ZooKeeperSession.getInstance();
        // 1.将对应的taskid写入zk的node中，形成列表
        // 2.将自己的热门商品列表写入对应的zk节点
        // 3.这样的话，并行的预热程序能从第一步中知道有哪些taskid
        // 4.并行的预热程序根据每个taskid去 获取锁，然后从对应的znode中拿到热门商品列表
        this.taskId = context.getThisTaskId();
        initTaskId(taskId);
    }

    private void initTaskId(int taskId) {
        // ProductCountBolt所有的task启动的时候， 都会将自己的taskid写到同一个node的值中
        // 格式就是逗号分隔，拼接成一个列表
        // 111,211,355
        zkSession.acquireDistributedLock();
        String taskidList = zkSession.getNodeData();
        LOGGER.info("【ProductCountBolt获取到taskid list】taskidList=" + taskidList);
        if (!"".equals(taskidList)) {
            taskidList += "," + taskId;
        } else {
            taskidList += taskId;
        }
        zkSession.setNodeData("/taskid-list", taskidList);
        LOGGER.info("【ProductCountBolt设置taskid list】taskidList=" + taskidList);
        zkSession.releaseDistributedLock();
    }

    public void execute(Tuple tuple) {
        Long productId = tuple.getLongByField("productId");
        LOGGER.info("ProductCountBolt接收到一条商品Id信息，productId=" + productId);
        Long count = productCountMap.get(productId);
        if (count == null) {
            count = 0L;
        }
        count++;
        productCountMap.put(productId, count);
        LOGGER.info("ProductCountBolt完成一条商品Id信息统计，productId=" + count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private class ProductCountThread implements Runnable {
        public void run() {
            ArrayList<Map.Entry<Long, Long>> topnProductList = new ArrayList<Map.Entry<Long, Long>>();
            int topn = 3;

            while (true) {
                topnProductList.clear();
                LOGGER.info("【ProductCountThread打印productCountMap的长度】size=" + productCountMap.size());
                for (Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {
                    if (topnProductList.size() == 0) {
                        topnProductList.add(productCountEntry);
                    } else {
                        boolean bigger = false;
                        for (int i = 0; i < topnProductList.size(); i++) {
                            Map.Entry<Long, Long> topnProductCountEntry = topnProductList.get(i);
                            if (productCountEntry.getValue() > topnProductCountEntry.getValue()) {
                                int lastIndex = topnProductList.size() < topn ? topnProductList.size() - 1 : topn - 2;
                                for (int j = lastIndex; j >= i; j--) {
                                    topnProductList.set(j + 1, topnProductList.get(j));
                                }
                                topnProductList.set(i, productCountEntry);
                                bigger = true;
                                break;
                            }
                        }

                        if (!bigger) {
                            if (topnProductList.size() < topn) {
                                topnProductList.add(productCountEntry);
                            }
                        }
                    }
                }
                String json = JSONArray.toJSONString(topnProductList);
                zkSession.setNodeData("task-hot-product-list-" + taskId, json);
                LOGGER.info("【ProductCountThread计算出一份top3热门商品列表】zk path=" + ("/task-hot-product-list-" + taskId) + ", topnProductListJSON=" + json);
                Utils.sleep(5000);
            }
        }
    }
}
