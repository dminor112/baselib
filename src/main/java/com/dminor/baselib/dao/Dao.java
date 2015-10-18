package com.dminor.baselib.dao;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.stereotype.Repository;
import javax.annotation.Resource;

/**
 * Description: 基本dao,使用这个dao需要继承本dao，然后封装各个实体对应的dao
 */
@Repository
public class Dao<T> extends AbstractGenericDao<T> {

    @Resource
    SessionFactory sessionFactory;

    @Override
    public Session getCurrentSession() {
        return sessionFactory.getCurrentSession();
    }

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }
}
