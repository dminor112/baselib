package com.dminor.baselib.dao;

import java.util.ArrayList;
import java.util.List;

/**
 * 涓�釜鍒嗛〉绫伙紝娌′粈涔堝彲璇寸殑锛屾櫘鏅�閫氬嵈鏂逛究浣跨敤
 * 
 * @author 闈冲箍鍗� * @since 1.0
 * @version 1.0
 */
public class Page<T> {
    /**
     * 褰撳墠绗嚑椤�     */
    private int page = 1;
    /**
     * 姣忛〉澶氬皯涓�     */
    private int pageSize = 20;
    /**
     * 鎬诲叡澶氬皯涓�     */
    private long allCount = 0;
    /**
     * 鍐呭
     */
    private List<T> list;

    public Page(int page, int pageSize, long allCount, List<T> list) {
        if (list == null) {
            list = new ArrayList<T>();
        }
        this.allCount = allCount;
        this.page = page;
        this.list = list;
        this.pageSize = pageSize;
    }

    /**
     * 鍙湁涓�〉鍐呭
     * 
     * @param list
     *            椤甸潰鍐呭
     */
    public Page(List<T> list) {
        if (list == null) {
            list = new ArrayList<T>();
        }
        this.allCount = list.size();
        this.page = 1;
        this.list = list;
        this.pageSize = list.size();
    }

    /**
     * 鑾峰彇褰撳墠椤垫暟
     */
    public int getPage() {
        return this.page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    /**
     * 鑾峰彇姣忛〉鏉℃暟
     */
    public int getPageSize() {
        return this.pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * 鑾峰彇鎬绘潯鏁�     */
    public long getAllCount() {
        return this.allCount;
    }

    public void setAllCount(long allCount) {
        this.allCount = allCount;
    }

    /**
     * 鑾峰彇褰撳墠椤垫暟鎹�     */
    public List<T> getList() {
        return this.list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }
}
