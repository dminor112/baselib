package com.dminor.baselib.dao;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public class BaseDao {

    @Autowired
    @Qualifier("hibernateSessionFactory")
    private SessionFactory sessionFactory;

    public Session getSession(){
        return sessionFactory.openSession();
    }

    public void closeSession(Session session){
    	if (session != null) {
			session.close();
		}
    }

}
