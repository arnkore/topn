package com.example.topn.heap;

import java.util.Comparator;
import java.util.Iterator;

/**
 * 小顶堆
 *
 * @author nevermore
 * @date 2019/3/12 下午3:18
 */
public class MinHeap<E extends Comparable<E>> implements Iterable<E> {
    private static final int ROOT = 1;

    private Comparator<E> comparator;

    private E[] elements;

    private int size;

    private int capacity;

    public MinHeap(int capacity, Comparator<E> comparator) {
        this.capacity = capacity;
        this.comparator = comparator;
        // 索引0处不存储元素，这种设计主要是为了方便计算左右孩子以及父节点。
        this.elements = (E[])new Comparable[capacity + 1];
    }

    public MinHeap(int capacity) {
        this(capacity, null);
    }

//    public MinHeap(E[] initElements) {
//        this(initElements, null);
//    }
//
//    public MinHeap(E[] initElements, Comparator<E> comparator) {
//        Objects.requireNonNull(initElements, "initElements is null");
//        this.size = initElements.length;
//        this.comparator = comparator;
//        // 索引0处不存储元素，这种设计主要是为了方便计算左右孩子以及父节点。
//        this.elements = (E[])new Comparable[size + 1];
//        System.arraycopy(initElements, 0, elements, 1, size);
//
//        for (int i = size / 2; i >= ROOT; i--) {
//            sinkDown(i);
//        }
//    }

    /**
     * 将堆中的最小元素替换为新元素
     *
     * @param ele
     * @return
     */
    public E replaceMin(E ele) {
        E res = elements[ROOT];
        elements[ROOT] = ele;
        sinkDown(ROOT);
        return res;
    }

    /**
     * 删除堆中的最小元素
     *
     * @return
     */
    public E delMin() {
        E ele = elements[size];
        size--;
        E res = replaceMin(ele);
        return res;
    }

    private void sinkDown(int ix) {
        while (ix <= size / 2) {
            int lc = ix * 2; // left child
            int rc = lc + 1; // right child
            int sc = lc; // smaller child

            if (rc <= size && less(rc, lc)) {
                sc = rc;
            }

            if (less(ix, sc)) {
                break;
            }
            swap(ix, sc);
            ix = sc;
        }
    }

    public void insert(E ele) {
        if (size >= capacity) {
            throw new HeapFullException();
        }
        elements[++size] = ele;
        floatUp(size);
    }

    private void floatUp(int ix) {
        int p = ix / 2;
        while (p >= ROOT && less(ix, p)) {
            swap(ix, p);
            ix = p;
        }
    }

    /**
     * 堆中索引i处的元素是否小于堆中索引j处的元素
     * @param i
     * @param j
     * @return
     */
    private boolean less(int i, int j) {
        if (comparator == null) {
            return elements[i].compareTo(elements[j]) < 0;
        } else {
            return comparator.compare(elements[i], elements[j]) < 0;
        }
    }

    /**
     * 交换索引i，j处的元素
     * @param i
     * @param j
     */
    private void swap(int i, int j) {
        E tmp = elements[i];
        elements[i] = elements[j];
        elements[j] = tmp;
    }

    public int size() {
        return size;
    }

    @Override
    public Iterator<E> iterator() {
        return new MinHeapIterator();
    }

    private class MinHeapIterator implements Iterator<E> {
        @Override
        public boolean hasNext() {
            return size() > 0;
        }

        @Override
        public E next() {
            return delMin();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
