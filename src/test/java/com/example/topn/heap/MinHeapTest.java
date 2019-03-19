package com.example.topn.heap;

import com.google.common.collect.Lists;
import com.example.topn.util.SortHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

/**
 * @author nevermore
 * @date 2019/3/12 下午4:13
 */
public class MinHeapTest {
    private Comparator<Integer> intComp = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
    };

    @Test
    public void testSortWithDistinctElements() {
        Integer[] arr = new Integer[]{9, 0, 3, 7, 1, 8, 6, 2, 4, 5};
        MinHeap<Integer> pq = new MinHeap(10);
        List<Integer> result = Lists.newArrayList();
        for (int e : pq) {
            result.add(e);
        }

        Assert.assertTrue(SortHelper.isSorted(result.toArray(arr)));
    }

    @Test
    public void testSortWithDuplicateElements() {
        Integer[] arr = new Integer[]{9, 0, 3, 0, 0, 8, 6, 1, 1, 5};
        MinHeap<Integer> pq = new MinHeap(10);
        List<Integer> result = Lists.newArrayList();
        for (int e : pq) {
            result.add(e);
        }

        Assert.assertTrue(SortHelper.isSorted(result.toArray(arr)));
    }

    @Test
    public void testSortWithAllSameElements() {
        Integer[] arr = new Integer[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        MinHeap<Integer> pq = new MinHeap(10);
        for (int e : pq) {
            Assert.assertEquals(0, e);
        }
    }

    @Test
    public void testMinHeap() {
        Integer[] arr = new Integer[]{9, 0, 3, 7, 1, 8, 6, 2, 4, 5};
        MinHeap<Integer> pq = new MinHeap(10);
        for (int i = 11; i <= 100; i++) {
            pq.replaceMin(i);
        }

        int k = 91;
        for (int e : pq) {
            Assert.assertEquals(k++, e);
        }
    }
}
