package io.stintflow.spi;

/** SDD 3b.5b: the key of an area was destroyed, so what was encrypted under it can no longer be read, nor anything written to it. */
public class AreaKeyDestroyedException extends RuntimeException {

    private final String area;

    public AreaKeyDestroyedException(String area) {
        super("The key of area '" + area + "' was destroyed");
        this.area = area;
    }

    public String area() {
        return area;
    }
}
