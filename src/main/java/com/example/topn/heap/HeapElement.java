package com.example.topn.heap;

import java.util.Comparator;

public class HeapElement implements Comparable<HeapElement> {
    private String url;

    private int repetitions;

    public HeapElement(String url, int repetitions) {
        this.url = url;
        this.repetitions = repetitions;
    }

    public String getUrl() {
        return url;
    }

    public int getRepetitions() {
        return repetitions;
    }

    @Override
    public int compareTo(HeapElement o) {
        if (o == null) {
            throw new IllegalArgumentException("Can't compare to null HeapElement!");
        }

        if (repetitions > o.repetitions) {
            return 1;
        } else if (repetitions < o.repetitions) {
            return -1;
        } else {
            return 0;
        }
    }

    public static final Comparator<HeapElement> HEAP_ELEMENT_COMPARATOR = new Comparator<HeapElement>() {
        @Override
        public int compare(HeapElement o1, HeapElement o2) {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Can't compare to null HeapElement!");
            }

            if (o1.repetitions > o2.repetitions) {
                return 1;
            } else if (o1.repetitions < o2.repetitions) {
                return -1;
            } else {
                return 0;
            }
        }
    };
}
