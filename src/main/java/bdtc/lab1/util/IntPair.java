package bdtc.lab1.util;

import java.io.Serializable;

/**
 * Объект для хранения интервалов температуры и работы с ней
 */
public class IntPair implements Serializable {
    private final int left;
    private final int right;

    public IntPair(int left, int right) {
        this.left = left;
        this.right = right;
    }

    public int getLeft() {
        return left;
    }

    public int getRight() {
        return right;
    }
}