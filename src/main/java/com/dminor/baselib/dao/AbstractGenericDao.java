package com.dminor.baselib.dao;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;

/**
* <p>
* 鍩烘湰鎶借薄娉涘瀷dao,
* 浣跨敤鏃讹紝璇峰搴旀瘡涓猻essionFactory缁ф壙姝ao鐢熸垚涓�釜鍩烘湰鎶借薄dao锛岀劧鍚庨拡瀵规瘡涓�釜浜簃odel瀵硅薄锛岀敓鎴愪竴涓搴旂殑瀹炰綋dao
* 锛岄�鐢ㄤ簬瀹炰綋鎿嶄綔杈冨鐗规畩鐨勬儏鍐碉紝 鑻ュ彧闇�鍩烘湰鐨勫鍒犳煡鏀规柟娉曪紝鍙互閫傜敤闈炴硾鍨媎ao
* </p>
* <p>
* <b>浣跨敤鏂规硶锛�/b> 棣栧厛锛屽湪浣犵殑绯荤粺涓疄鐜颁竴涓狟aseDao锛岀户鎵挎绫伙紝瀹炵幇{@link #getCurrentSession()}
* 鏂规硶,鐒跺悗姣斿浣犳湁涓�釜瀵瑰簲鏁版嵁搴撹〃鐨勫疄浣撶被User
* 锛岄偅涔堜綘闇�鍐欎竴涓猆serDao骞剁户鎵緽aseDao绫伙紝鍦ㄦ绫讳腑瀹炵幇棰濆鐨勯渶瑕侊紝鐒跺悗涓婂眰service璋冪敤userDao銆� * 
* @author 闈冲箍鍗� * @since 1.0
* @version 1.0
*/
@SuppressWarnings("unchecked")
public abstract class AbstractGenericDao<T> {
   private Class<T> clazz;

   /**
    * 鍔犺浇鎸囧畾ID鐨勬寔涔呭寲瀵硅薄
    **/
   public T get(Serializable id) {
       return (T) getCurrentSession().get(getTclass(), id);
   }

   /**
    * 鏇存柊瀵硅薄
    */
   public void update(T obj) {
       getCurrentSession().update(obj);
   }

   /**
    * 淇濆瓨瀵硅薄
    * 
    * @return 杩斿洖涓婚敭
    */
   public Serializable save(T obj) {
       return getCurrentSession().save(obj);
   }

   /**
    * 淇濆瓨鎴栨洿鏂版寚瀹氱殑鎸佷箙鍖栧璞�     **/
   public void saveOrUpdate(T obj) {
       getCurrentSession().saveOrUpdate(obj);
   }

   /**
    * 鍒犻櫎鎸囧畾ID鐨勬寔涔呭寲瀵硅薄
    * 
    * @param id
    *            涓婚敭
    **/
   public void delete(Serializable id) {
       Session session = getCurrentSession();
       Object obj = session.get(getTclass(), id);
       if (obj != null) {
           session.delete(obj);
       }
   }

   /**
    * 鍒犻櫎鎸囧畾鐨勬寔涔呭寲瀵硅薄
    * 
    * @param obj
    *            瑕佸垹闄ょ殑瀵硅薄
    */
   public void delete(T obj) {
       getCurrentSession().delete(obj);
   }

   /**
    * 鏌ヨ鎸囧畾绫荤殑婊¤冻鏉′欢鐨勬寔涔呭寲瀵硅薄
    * 
    * @param hql
    *            瑕佹煡璇㈢殑hql璇彞
    **/
   public List<T> query(String hql) {
       return getCurrentSession().createQuery(hql).list();
   }

   /**
    * 鎸夌収hql璇彞鏌ュ鎸囧畾椤电殑鏁版嵁
    * 
    * @param hql
    *            hql璇彞,鍙帴鍙楃畝鍗昲ql璇彞锛屼笉搴斿寘鍚玤roup by瀛愬彞
    * @param page
    *            椤垫暟
    * @param pageSize
    *            姣忛〉鏉℃暟
    * @return 杩斿洖涓�〉鏁版嵁
    */
   public Page<T> query(String hql, int page, int pageSize) {
       List<T> list = getCurrentSession().createQuery(hql).setMaxResults(pageSize).setFirstResult((page - 1) * pageSize).list();
       long allCount = count(hql);
       return new Page<T>(page, pageSize, allCount, list);
   }

   /**
    * 鏌ヨ鎵�湁
    * 
    * @return 杩斿洖鎵�湁鏁版嵁
    */
   public List<T> list() {
       return getCurrentSession().createCriteria(getTclass()).list();
   }

   /**
    * 渚濇嵁鎸囧畾鐨勬潯浠惰緭鍑�     * 
    * @param wheres
    *            鎸囧畾鐨勬潯浠躲�<b>eg.</b>
    *            org.hibernate.criterion.Restrictions.eq("testPro", 1);
    * @return 杩斿洖鏌ヨ缁撴灉
    */
   public List<T> list(Criterion... wheres) {
       Criteria criteria = getCurrentSession().createCriteria(getTclass());
       if (wheres != null) {
           for (Criterion c : wheres) {
               if (c != null) {
                   criteria.add(c);
               }
           }
       }
       return criteria.list();
   }

   /**
    * 鏌ヨ鎵�湁锛屾寜鐓ф寚瀹氬瓧娈垫帓搴�     * 
    * @param orderColumn
    *            鎺掑簭瀛楁
    * @param isAsc
    *            鏄惁姝ｅ簭
    * @return 杩斿洖鏌ヨ缁撴灉
    */
   public List<T> list(String orderColumn, boolean isAsc) {
       Criteria criteria = getCurrentSession().createCriteria(getTclass());
       if (isAsc) {
           criteria.addOrder(Order.asc(orderColumn));
       } else {
           criteria.addOrder(Order.desc(orderColumn));
       }
       return criteria.list();
   }

   /**
    * 渚濇嵁鎸囧畾鐨勬潯浠舵煡璇�     * 
    */
   public List<T> list(String property, Object value) {
       Criteria criteria = getCurrentSession().createCriteria(getTclass());
       criteria.add(Restrictions.eq(property, value));
       return criteria.list();
   }

   /**
    * 鎸夌収鎸囧畾鐨勬潯浠跺拰鎺掑簭鏌ヨ
    * 
    * @param property
    *            瑕佺瓫閫夌殑灞炴�銆備細鎸夌収姝ゅ睘鎬�=value鍙傛暟鎸囧畾鍊艰繘琛岀瓫閫�     * @param value
    *            绛涢�鏉′欢
    * @param orderColumn
    *            鎺掑簭瀛楁锛宮odel灞炴�鍚�     * @param isAsc
    *            鏄惁姝ｅ簭
    * @return 杩斿洖鏌ヨ缁撴灉
    */
   public List<T> list(String orderColumn, boolean isAsc, String property, Object value) {
       Criteria criteria = getCurrentSession().createCriteria(getTclass());
       criteria.add(Restrictions.eq(property, value));
       if (isAsc) {
           criteria.addOrder(Order.asc(orderColumn));
       } else {
           criteria.addOrder(Order.desc(orderColumn));
       }
       return criteria.list();
   }

   public List<T> list(String orderColumn, boolean isAsc, String property1, Object value1, String property2, Object value2) {
       Criteria criteria = getCurrentSession().createCriteria(getTclass());
       criteria.add(Restrictions.eq(property1, value1));
       criteria.add(Restrictions.eq(property2, value2));
       if (isAsc) {
           criteria.addOrder(Order.asc(orderColumn));
       } else {
           criteria.addOrder(Order.desc(orderColumn));
       }
       return criteria.list();
   }

   /**
    * 渚濇嵁鎸囧畾鐨勬潯浠跺拰鎺掑簭杈撳嚭
    * 
    * @param orderColumn
    *            鎺掑簭瀛楁鍚嶏紝model灞炴�
    * @param isAsc
    *            鏄惁姝ｅ簭
    * @param wheres
    *            鎸囧畾鐨勬潯浠躲�<b>eg.</b>
    *            org.hibernate.criterion.Restrictions.eq("testPro", 1);
    * @return 杩斿洖鏌ヨ缁撴灉
    */
   public List<T> list(String orderColumn, boolean isAsc, Criterion... wheres) {
       Criteria criteria = getCurrentSession().createCriteria(getTclass());
       if (wheres != null) {
           for (Criterion c : wheres) {
               if (c != null) {
                   criteria.add(c);
               }
           }
       }
       criteria.addOrder(isAsc ? Order.asc(orderColumn) : Order.desc(orderColumn));
       return criteria.list();
   }

   /**
    * 璁℃暟
    */
   public long count() {
       Criteria criteria = getCurrentSession().createCriteria(getTclass());
       return ((Long) criteria.setProjection(Projections.rowCount()).uniqueResult()).longValue();
   }

   /**
    * 璁℃暟
    * 
    * @param hql
    *            鎸囧畾鐨刪ql璇彞,鍙帴鍙楃畝鍗昲ql璇彞锛屼笉搴斿寘鍚玤roup by瀛愬彞
    * @return 杩斿洖鏌ヨ缁撴灉
    */
   public long count(String hql) {
       if (hql.trim().startsWith("from")) {
           hql = "select count(*) " + hql;
       }
       Long count = (Long) getCurrentSession().createQuery(hql).iterate().next();
       return count == null ? 0 : count.longValue();
   }

   /**
    * 璁℃暟
    * 
    * @param wheres
    *            鎸囧畾鐨勬潯浠躲�<b>eg.</b>
    *            org.hibernate.criterion.Restrictions.eq("testPro", 1);
    * @return 杩斿洖鏌ヨ缁撴灉
    */
   public long count(Criterion... wheres) {
       Criteria criteria = getCurrentSession().createCriteria(getTclass());
       if (wheres != null) {
           for (Criterion c : wheres) {
               if (c != null) {
                   criteria.add(c);
               }
           }
       }
       return ((Long) criteria.setProjection(Projections.rowCount()).uniqueResult()).longValue();
   }

   /**
    * 鑾峰彇鍒嗛〉
    * 
    * @param page
    *            椤垫暟
    * @param pageSize
    *            姣忛〉鏉℃暟
    * @return 杩斿洖鏌ヨ缁撴灉
    */
   public Page<T> page(int page, int pageSize) {
       List<T> list = list();
       return new Page<T>(page, pageSize, count(), list);
   }

   /**
    * 婊¤冻鎸囧畾鐨勬潯浠剁殑鍒嗛〉
    * 
    * @param page
    *            椤垫暟
    * @param pageSize
    *            姣忛〉鏉℃暟
    * @param wheres
    *            瑕佹煡璇㈢殑鏉′欢
    * @return 杩斿洖鏌ヨ缁撴灉
    */
   public Page<T> page(int page, int pageSize, Criterion... wheres) {
       Criteria criteria = getCurrentSession().createCriteria(getTclass());
       if (wheres != null) {
           for (Criterion c : wheres) {
               if (c != null) {
                   criteria.add(c);
               }
           }
       }
       criteria.setFirstResult((page - 1) * pageSize).setMaxResults(pageSize);
       return new Page<T>(page, pageSize, count(wheres), criteria.list());
   }

   /**
    * 鎸夌収鎸囧畾鐨勬帓搴忚幏鍙栧垎椤�     * 
    * @param page
    *            椤垫暟
    * @param pageSize
    *            姣忛〉鏉℃暟
    * @param orderColumn
    *            鎺掑簭瀛楁
    * @param isAsc
    *            鏄惁姝ｅ簭
    * @return 杩斿洖鏌ヨ缁撴灉
    */
   public Page<T> page(int page, int pageSize, String orderColumn, boolean isAsc) {
       List<T> list = list(page, pageSize, orderColumn, isAsc);
       return new Page<T>(page, pageSize, count(), list);
   }

   /**
    * 鎸夌収鎸囧畾鐨勬帓搴忥紝骞舵弧瓒虫寚瀹氱殑鏉′欢鐨勫垎椤�     * 
    * @param page
    *            椤垫暟
    * @param pageSize
    *            姣忛〉鏉℃暟
    * @param orderColumn
    *            鎺掑簭瀛楁锛宮odel灞炴�鍚�     * @param isAsc
    *            鏄惁姝ｅ簭
    * @param property
    *            闇�绛涢�鐨勫睘鎬у悕锛屼細绛涢�姝ゅ睘鎬�=value鍙傛暟鍊煎緱缁撴灉
    * @param value
    *            鎸夌収鎸囧畾鐨勫睘鎬х殑鍊�     * @return 杩斿洖鏌ヨ缁撴灉
    */
   public Page<T> page(int page, int pageSize, String orderColumn, boolean isAsc, String property, Object value) {
       List<T> list = list(page, pageSize, orderColumn, isAsc, Restrictions.eq(property, value));
       return new Page<T>(page, pageSize, count(Restrictions.eq(property, value)), list);
   }

   /**
    * 鎸夌収鎸囧畾鐨勬帓搴忥紝骞舵弧瓒虫寚瀹氱殑鏉′欢鐨勫垎椤�     * 
    * @param page
    *            椤垫暟
    * @param pageSize
    *            姣忛〉鏉℃暟
    * @param orderColumn
    *            鎺掑簭瀛楁锛宮odel灞炴�鍚�     * @param isAsc
    *            鏄惁姝ｅ簭
    * @param property1
    *            闇�绛涢�鐨勫睘鎬у悕锛屼細绛涢�姝ゅ睘鎬�=value鍙傛暟鍊煎緱缁撴灉
    * @param value1
    *            鎸夌収鎸囧畾鐨勫睘鎬х殑鍊�     * @param property2
    *            闇�绛涢�鐨勫睘鎬у悕2
    * @param value2
    *            鎸囧畾灞炴�鐨勫�
    * @return 杩斿洖鏌ヨ缁撴灉
    */
   public Page<T> page(int page, int pageSize, String orderColumn, boolean isAsc, String property1, Object value1, String property2, Object value2) {
       List<T> list = list(page, pageSize, orderColumn, isAsc, Restrictions.eq(property1, value1), Restrictions.eq(property2, value2));
       return new Page<T>(page, pageSize, count(Restrictions.eq(property1, value1), Restrictions.eq(property2, value2)), list);
   }

   /**
    * 鎸夌収鎸囧畾鐨勬潯浠跺垎椤靛拰鎺掑簭鏂瑰紡鍒嗛〉
    * 
    * @param page
    *            椤垫暟
    * @param pageSize
    *            姣忛〉鏉℃暟
    * @param wheres
    *            鎸囧畾鐨勬潯浠躲�<b>eg.</b>
    *            org.hibernate.criterion.Restrictions.eq("testPro", 1);
    * @param orderColumn
    *            鎺掑簭瀛楁锛宮odel灞炴�鍚�     * @param isAsc
    *            鏄惁姝ｅ簭
    * @return 杩斿洖鏌ヨ缁撴灉
    */
   public Page<T> page(int page, int pageSize, String orderColumn, boolean isAsc, Criterion... wheres) {
       List<T> list = list(page, pageSize, orderColumn, isAsc, wheres);
       return new Page<T>(page, pageSize, count(wheres), list);
   }

   /**
    * 鎵ц鎸囧畾鐨剆ql璇彞
    * 
    * @param sql
    * @return 杩斿洖褰卞搷鐨勮鏁�     */
   public int updateSql(String sql) {
       return getCurrentSession().createSQLQuery(sql).executeUpdate();
   }

   /**
    * 鑾峰彇娉涘瀷绫诲瀷 娉涘瀷绫诲瀷涓嶅彲浠ュ湪鍒濆鍖栨湡闂磋幏鍙栵紝鍙兘鍦ㄥ垵濮嬪寲瀹屾垚纭畾绫诲瀷浠ュ悗鎵嶅彲浠ヨ幏鍙栧埌娉涘瀷
    */
   private Class<T> getTclass() {
       if (clazz == null) {
           clazz = (Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
       }
       return clazz;
   }

   /**
    * 鑾峰彇鏁版嵁搴搒ession锛屽鏋滅郴缁熶腑娉ㄥ叆鐨勬椂sessionFactory锛屼綘搴旇杩斿洖
    * sessionFactory.getCurrentSession()
    * 
    * @return 杩斿洖褰撳墠session銆�     */
   protected abstract Session getCurrentSession();

   private List<T> list(int page, int pageSize, String orderColumn, boolean isAsc, Criterion... wheres) {
       Criteria criteria = getCurrentSession().createCriteria(getTclass());
       if (wheres != null) {
           for (Criterion c : wheres) {
               if (c != null) {
                   criteria.add(c);
               }
           }
       }
       criteria.addOrder(isAsc ? Order.asc(orderColumn) : Order.desc(orderColumn));
       criteria.setFirstResult((page - 1) * pageSize).setMaxResults(pageSize);
       return criteria.list();
   }
}

