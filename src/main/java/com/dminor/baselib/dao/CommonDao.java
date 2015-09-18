package com.dminor.baselib.dao;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.jdbc.Work;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.dminor.baselib.utils.JsonUtil;

@Repository("commonDao")
public class CommonDao extends BaseDao{

	private static Logger log = LoggerFactory.getLogger("cmsinfo");

    public boolean save(Object entity){
    	if(entity==null){
    		return false;
    	}
        Session session = null;
        try{
            session = getSession();
            session.save(entity);
            session.flush();
            return true;
        }catch(Exception e){
            log.error("CommonDao saveOjbect exception:"+e.toString(), e);
            return false;
        }finally{
            closeSession(session);
        }
    }
    
    
    public boolean save(List<?> list){
        if(list==null || list.isEmpty()){
        	return false;
        }
        Session session = null;
        try{
        	session = getSession();
        	int size=list.size();
        	Object obj=null;
        	for(int i=0;i<size;i++){
        		obj=list.get(i);
        		if(obj!=null){
        			session.save(obj);
        		}
        		if (i % 30 == 0)
    			{
    				session.flush();
    				session.clear();
    			}
        	}
        	session.flush();
        	session.clear();
            return true;
        }catch(Exception e){
        	log.error("CommonDao saveList exception:"+e.toString(), e);
            return false;
        }finally{
            closeSession(session);
        }

    }
    
    
    public Long saveAndReturnObj(Object obj){
    	if(obj==null){
    		return null;
    	}
        Session session = null;
        try{
            session = getSession();
            obj=session.save(obj);
            session.flush();
            return (Long)obj;
        }catch(Exception e){
            log.error("CommonDao saveAndReturnObj exception:"+e.toString(), e);
            return null;
        }finally{
            closeSession(session);
        }

    }


    public boolean update(Object entity){
    	if(entity==null){
    		return false;
    	}
        Session session = null;
        try{
            session = getSession();
            session.update(entity);
            session.flush();
            return true;
        }catch(Exception e){
        	log.error("CommonDao updateObject exception:"+e.toString(), e);
            return false;
        }finally{
            closeSession(session);
        }
    }
    
    public boolean update(List<?> list){
        if(list==null || list.isEmpty()){
        	return false;
        }
        Session session = null;
        try{
        	session = getSession();
        	int size=list.size();
        	Object obj=null;
        	for(int i=0;i<size;i++){
        		obj=list.get(i);
        		if(obj!=null){
        			session.update(obj);
        		}
        		if (i % 30 == 0)
    			{
    				session.flush();
    				session.clear();
    			}
        	}
        	session.flush();
        	session.clear();
            return true;
        }catch(Exception e){
        	log.error("CommonDao saveList exception:"+e.toString(), e);
            return false;
        }finally{
            closeSession(session);
        }

    }


    public boolean saveOrUpdate(Object entity){
    	if(entity==null){
    		return false;
    	}
        Session session = null;
        try{
            session = getSession();
            session.saveOrUpdate(entity);
            session.flush();
            return true;
        }catch(Exception e){
        	log.error("CommonDao saveOrUpdateObject exception:"+e.toString(), e);
            return false;
        }finally{
            closeSession(session);
        }
    }
    
    
    public boolean saveOrUpdate(List<?> list){
        if(list==null || list.isEmpty()){
        	return false;
        }
        Session session = null;
        try{
        	session = getSession();
        	int size=list.size();
        	Object obj=null;
        	for(int i=0;i<size;i++){
        		obj=list.get(i);
        		if(obj!=null){
        			session.saveOrUpdate(obj);
        		}
        		if (i % 30 == 0)
    			{
    				session.flush();
    				session.clear();
    			}
        	}
        	session.flush();
        	session.clear();
            return true;
        }catch(Exception e){
        	log.error("CommonDao saveOrUpdateList exception:"+e.toString(), e);
            return false;
        }finally{
            closeSession(session);
        }

    }
    

    public Object get(Class<?> clazz,Serializable id){
    	if(clazz==null || id==null){
    		return null;
    	}
        Session session = null;
        try{
            session = getSession();
            return session.get(clazz, id);
        }catch(Exception e){
        	log.error("CommonDao get exception:"+e.toString(), e);
            return null;
        }finally{
            closeSession(session);
        }
    }
    
    @SuppressWarnings("unchecked")
	public <T> T getBean(T t,Serializable id){
    	if(t==null || id==null){
    		return null;
    	}
        Session session = null;
        try{
            session = getSession();
            return (T) session.get(t.getClass(), id);
        }catch(Exception e){
        	log.error("CommonDao getBean exception:"+e.toString(), e);
            return null;
        }finally{
            closeSession(session);
        }
    }
    
    
    

	@SuppressWarnings("unchecked")
	public <T> List<T> query(String queryString,Object[] params){
		if(queryString ==null || queryString.isEmpty()){
			return null;
		}
        Session session = null;
        try{
            session = getSession();
            Query query=session.createQuery(queryString);
            if(params!=null && params.length>0){
                for(int i=0;i<params.length;i++){
                    query.setParameter(i, params[i]);
                }
            }
            List<T> list=query.list();
            return list;
        }catch (HibernateException e) {
			throw e;
		}catch(Exception e){
        	log.error("CommonDao query exception:"+e.toString(), e);
            return null;
        }finally{
            closeSession(session);
        }
    }
	
	public Integer batch(String queryString,Object[] params){
		if(queryString ==null || queryString.isEmpty()){
			return null;
		}
        Session session = null;
        try{
            session = getSession();
            Query query=session.createQuery(queryString);
            if(params!=null && params.length>0){
                for(int i=0;i<params.length;i++){
                    query.setParameter(i, params[i]);
                }
            }
            
            return query.executeUpdate();
        }catch(Exception e){
        	log.error("CommonDao batch exception:"+e.toString(), e);
            return null;
        }finally{
            closeSession(session);
        }
    }
	
	
	
    public ArrayNode jdbcQuery(final String queryString){
    	if(queryString ==null || queryString.isEmpty()){
			return null;
		}
        Session session = null;
        try{
	
        	final ObjectMapper objectMapper=new ObjectMapper();
        	final ArrayNode arrayNode=objectMapper.createArrayNode();
            session = getSession();
            Work work=new Work() {
				public void execute(Connection connection) throws SQLException {
					PreparedStatement stmt=connection.prepareStatement(queryString);
					ResultSet set=stmt.executeQuery();
					int size=set.getMetaData().getColumnCount();
					ResultSetMetaData result=set.getMetaData();
					
					
					ObjectNode objectNode=null;
					while(set.next()){
						objectNode=objectMapper.createObjectNode();
						for(int i=1;i<=size;i++){
							objectNode.put(result.getColumnLabel(i),set.getString(i));
						}
						arrayNode.add(objectNode);
					}
				}
			};
			session.doWork(work);
            return arrayNode;
        }catch(Exception e){
        	log.error("CommonDao jdbcQuery exception:"+e.toString(), e);
            return null;
        }finally{
            closeSession(session);
        }
    }
    
    
    public ArrayNode jdbcQuery(final String queryString,final Object[] params,final ObjectMapper objectMapper){
    	if(queryString ==null || queryString.isEmpty()){
			return null;
		}
        Session session = null;
        try{
        	final ArrayNode arrayNode=objectMapper.createArrayNode();
            session = getSession();
            Work work=new Work() {
				public void execute(Connection connection) throws SQLException {
					PreparedStatement stmt=connection.prepareStatement(queryString);
					Class<?> integerClass=Integer.class;
					Class<?> floatClass=Float.class;
					Class<?> doubleClass=Double.class;
					Class<?> longClass=Long.class;
					
					Class<?> booleanClass=Boolean.class;
					Class<?> byteClass=Byte.class;
					Class<?> shortClass=Short.class;
					Class<?> charClass=Character.class;
					Object object=null;
					for(int i=0;i<params.length;i++){
						stmt.setObject(i+1, params[i]);
					}
					ResultSet set=stmt.executeQuery();
					int size=set.getMetaData().getColumnCount();
					ResultSetMetaData result=set.getMetaData();
					
					
					ObjectNode objectNode=null;
					while(set.next()){
						objectNode=objectMapper.createObjectNode();
						for(int i=1;i<=size;i++){
							object=set.getObject(i);
							if(object!=null){
                                if(object.getClass()==String.class && !"".equals(object.toString())){
                                	objectNode.put(result.getColumnLabel(i),object.toString());
								}
                                else if(object.getClass()==integerClass){
									objectNode.put(result.getColumnLabel(i),(Integer)object);
								}
								else if(object.getClass()==longClass){
									objectNode.put(result.getColumnLabel(i),(Long)object);
								}
								else if(object.getClass()==floatClass){
									objectNode.put(result.getColumnLabel(i),JsonUtil.objectToJsonNode((Float)object));
								}
								else if(object.getClass()==doubleClass){
									objectNode.put(result.getColumnLabel(i),JsonUtil.objectToJsonNode((Double)object));
								}
								else if(object.getClass()==booleanClass){
									objectNode.put(result.getColumnLabel(i),(Boolean)object);
								}
								else if(object.getClass()==byteClass){
									objectNode.put(result.getColumnLabel(i),(Byte)object);
								}
								else if(object.getClass()==shortClass){
									objectNode.put(result.getColumnLabel(i),(Short)object);
								}
								else if(object.getClass()==charClass){
									objectNode.put(result.getColumnLabel(i),(Character)object);
								}
								else{
									if(object.getClass()!=String.class){
										objectNode.put(result.getColumnLabel(i),object.toString());
									}
								}
							}
							/*else{
								objectNode.putPOJO(result.getColumnLabel(i),null);
							}*/
							
						}
						arrayNode.add(objectNode);
					}
				}
			};
			session.doWork(work);
            return arrayNode;
        }catch (HibernateException e) {
			throw e;
		}catch(Exception e){
        	log.error("CommonDao jdbcQueryObjectMapper exception:"+e.toString(), e);
            return null;
        }finally{
            closeSession(session);
        }
    }
    
    
    public ArrayNode jdbcV3Query(final String queryString,final Object[] params,final ObjectMapper objectMapper){
    	if(queryString ==null || queryString.isEmpty()){
			return null;
		}
        Session session = null;
        try{
        	final ArrayNode arrayNode=objectMapper.createArrayNode();
            session = getSession();
            Work work=new Work() {
				public void execute(Connection connection) throws SQLException {
					PreparedStatement stmt=connection.prepareStatement(queryString);
					Class<?> integerClass=Integer.class;
					Class<?> longClass=Long.class;
					Class<?> floatClass=Float.class;
					Class<?> doubleClass=Double.class;
					
					Class<?> booleanClass=Boolean.class;
					Class<?> byteClass=Byte.class;
					Class<?> shortClass=Short.class;
					Class<?> charClass=Character.class;
					Object object=null;
					for(int i=0;i<params.length;i++){
						stmt.setObject(i+1, params[i]);
					}
					ResultSet set=stmt.executeQuery();
					int size=set.getMetaData().getColumnCount();
					ResultSetMetaData result=set.getMetaData();
					
					
					ObjectNode objectNode=null;
					while(set.next()){
						objectNode=objectMapper.createObjectNode();
						for(int i=1;i<=size;i++){
							object=set.getObject(i);
							if(object!=null){
								if(object.getClass()==integerClass){
									objectNode.put(result.getColumnLabel(i),(Integer)object);
								}
								else if(object.getClass()==longClass){
									objectNode.put(result.getColumnLabel(i),(Long)object);
								}
								else if(object.getClass()==floatClass){
									objectNode.put(result.getColumnLabel(i),JsonUtil.objectToJsonNode((Float)object));
								}
								else if(object.getClass()==doubleClass){
									objectNode.put(result.getColumnLabel(i),JsonUtil.objectToJsonNode((Double)object));
								}
								else if(object.getClass()==booleanClass){
									objectNode.put(result.getColumnLabel(i),(Boolean)object);
								}
								else if(object.getClass()==byteClass){
									objectNode.put(result.getColumnLabel(i),(Byte)object);
								}
								else if(object.getClass()==shortClass){
									objectNode.put(result.getColumnLabel(i),(Short)object);
								}
								else if(object.getClass()==charClass){
									objectNode.put(result.getColumnLabel(i),(Character)object);
								}
								else{
									objectNode.put(result.getColumnLabel(i),object.toString());
								}
							}
							else{
								if("java.lang.Integer".equals(result.getColumnClassName(i)) || "java.lang.Float".equals(result.getColumnClassName(i)) ||
										"java.lang.Long".equals(result.getColumnClassName(i)) || "java.lang.Double".equals(result.getColumnClassName(i)) ||
										"java.lang.Byte".equals(result.getColumnClassName(i)) || "java.lang.short".equals(result.getColumnClassName(i))||
										"java.lang.Character".equals(result.getColumnClassName(i))){
									
									objectNode.put(result.getColumnLabel(i),0);
								}
								else if("java.lang.Boolean".equals(result.getColumnClassName(i))){
									objectNode.put(result.getColumnLabel(i),false);
								}
								else{
									objectNode.put(result.getColumnLabel(i),"");
								}
							}
							
						}
						arrayNode.add(objectNode);
					}
				}
			};
			session.doWork(work);
            return arrayNode;
        }catch (HibernateException e) {
        	log.error("the DB has problem.HibernateException:", e);
			throw e;
		}catch(Exception e){
        	log.error("CommonDao jdbcV3QueryObjectMapper exception:"+e.toString(), e);
            return null;
        }finally{
            closeSession(session);
        }
    }
    
    
    public ArrayNode jdbcQueryString(final String queryString,final Object[] params){
    	if(queryString ==null || queryString.isEmpty()){
			return null;
		}
        Session session = null;
        try{
	
        	final ObjectMapper objectMapper=new ObjectMapper();
        	final ArrayNode arrayNode=objectMapper.createArrayNode();
            session = getSession();
            Work work=new Work() {
				public void execute(Connection connection) throws SQLException {
					PreparedStatement stmt=connection.prepareStatement(queryString);
					
					for(int i=0;i<params.length;i++){
						stmt.setObject(i+1, params[i]);
					}
					
					ResultSet set=stmt.executeQuery();
					int size=set.getMetaData().getColumnCount();
					ResultSetMetaData result=set.getMetaData();
					
					
					ObjectNode objectNode=null;
					while(set.next()){
						objectNode=objectMapper.createObjectNode();
						for(int i=1;i<=size;i++){
							objectNode.put(result.getColumnLabel(i),set.getString(i));
						}
						arrayNode.add(objectNode);
					}
				}
			};
			session.doWork(work);
            return arrayNode;
        }catch(Exception e){
        	log.error("CommonDao jdbcQueryString exception:"+e.toString(), e);
            return null;
        }finally{
            closeSession(session);
        }
    }


    @SuppressWarnings("unchecked")
	public <T> List<T> queryByPage(String queryString,Object[] params,int firstResult,int maxResults){
    	if(queryString ==null || queryString.isEmpty()){
			return null;
		}
        Session session = null;
        try{
            session = getSession();
            Query query=session.createQuery(queryString);
            if(params!=null && params.length>0){
                for(int i=0;i<params.length;i++){
                    query.setParameter(i, params[i]);
                }
            }
            query.setFirstResult(firstResult);
            query.setMaxResults(maxResults);
            List<T> list=query.list();
            return list;
        }catch (HibernateException e) {
			throw e;
		}catch(Exception e){
        	log.error("CommonDao queryByPage exception:"+e.toString(), e);
            return null;
        }finally{
            closeSession(session);
        }
    }
    
    

    public boolean delete(Object entity){
    	if(entity==null){
    		return false;
    	}
        Session session = null;
        try{
            session = getSession();
            session.delete(entity);
            session.flush();
            return true;
        }catch(Exception e){
        	log.error("CommonDao delete exception:"+e.toString(), e);
            return false;
        }finally{
            closeSession(session);
        }
    }

    public Long count(String queryString,Object[] params){
    	if(queryString ==null || queryString.isEmpty()){
			return null;
		}
        Session session = null;
        try{
            session = getSession();
            Query query=session.createQuery(queryString);
            if(params!=null && params.length>0){
                for(int i=0;i<params.length;i++){
                    query.setParameter(i, params[i]);
                }
            }
            return (Long)query.iterate().next();
        }catch(Exception e){
        	log.error("CommonDao count exception:"+e.toString(), e);
            return null;
        }finally{
            closeSession(session);
        }

    }
    
    public void executeUpdate(String queryString,Object[] params){
    	if(queryString ==null || queryString.isEmpty()){
			return;
		}
        Session session = null;
        try{
            session = getSession();
            Query query=session.createQuery(queryString);
            if(params!=null && params.length>0){
                for(int i=0;i<params.length;i++){
                    query.setParameter(i, params[i]);
                }
            }
            query.executeUpdate();
        }catch(Exception e){
        	log.error("CommonDao count exception:"+e.toString(), e);
        }finally{
            closeSession(session);
        }
    }
    
    public void setVideoStatus(String vidAndSite, String status){
    	Session session = null;
    	String vid = vidAndSite.split("_")[0];
		String site = vidAndSite.split("_")[1];
    	String hql = "";
    	if("1".equals(site)){
    		hql = "UPDATE t_api_video t SET t.status="+status+" WHERE vid="+vid+" and (site IS NULL or site=1)";
    	}else{
    		hql = "UPDATE t_api_video t SET t.status="+status+" WHERE vid="+vid+" and site="+site;
    	}
    	try{
            session = getSession();
            session.createSQLQuery(hql).executeUpdate();
            session.flush();
        }catch(Exception e){
        	log.error("CommonDao count exception:", e);
        }finally{
            closeSession(session);
        }
    }
    
    public void updateVideoAlbum(String oldAid, String newAid){
    	Session session = null;
    	String hql = "UPDATE t_api_video t SET t.aid=" + newAid + " WHERE aid=" + oldAid;
    	try{
            session = getSession();
            session.createSQLQuery(hql).executeUpdate();
            session.flush();
        }catch(Exception e){
        	log.error("CommonDao count exception:", e);
        }finally{
            closeSession(session);
        }
    }
}
