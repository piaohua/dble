package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.ServiceTask;

import java.util.concurrent.Executor;

/**
 * Created by szf on 2020/7/9.
 */
public class MySQLCurrentResponseService extends MySQLResponseService {

    public MySQLCurrentResponseService(AbstractConnection connection) {
        super(connection);
    }


    @Override
    public void TaskToTotalQueue(ServiceTask task) {
        if (isComplexQuery()) {
            super.TaskToTotalQueue(task);
        } else {
            if (isHandling.compareAndSet(false, true)) {
                DbleServer.getInstance().getConcurrentBackHandlerQueue().offer(task);
            }
        }
    }


    @Override
    public void consumerInternalData() {
        try {
            super.handleInnerData();
        } finally {
            isHandling.set(false);
        }
    }
}
