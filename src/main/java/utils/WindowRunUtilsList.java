package utils;
import java.util.LinkedList;
import java.util.List;

public class WindowRunUtilsList {

    private final List<WindowRunUtils> list;

    public WindowRunUtilsList() {
        this.list = new LinkedList<WindowRunUtils>();
    }

    public void addNewItem(WindowRunUtils item) {
        this.list.add(item);
    }

//    @Override
//    public String toString() {
//        StringBuilder output = new StringBuilder();
//
//
//        for (WindowRunUtils item: this.list) {
//            System.out.println(
//                String.format(
//
//                )
//            );
//        }
//
//        return output.toString();
//    }
}
