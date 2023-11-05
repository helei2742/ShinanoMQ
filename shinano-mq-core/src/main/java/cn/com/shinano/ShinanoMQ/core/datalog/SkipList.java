package cn.com.shinano.ShinanoMQ.core.datalog;

import java.util.*;

public class SkipList<T> {
    private SkipNode<T> headNode;
    private final Comparator<T> comparator;
    private int maxLevel;
    private static final int LEVEL_LIMIT = 5;

    public SkipList(Comparator<T> comparator) {
        this.headNode = new SkipNode<>(null, null, null);
        this.comparator = new Comparator<T>() {
            @Override
            public int compare(T o1, T o2) {
                if(o1 != null && o2 != null) return comparator.compare(o1,o2);
                else if(o1 == null) return -1;
                else return 1;
            }
        };
        this.maxLevel = 0;
    }

    public SkipNode<T> search(T target) {
        SkipNode<T> p = this.headNode;

        //找到最右边
        while (p != null) {
            int compare = comparator.compare(p.value, target);
            if (compare == 0) {
                return p;
            } else if (p.right == null) {
                p = p.down;
            } else if (comparator.compare(p.right.value, target) > 0) {
                p = p.down;
            } else {
                p = p.right;
            }
        }
        return null;
    }

    public void delete(T target) {
        SkipNode<T> p = headNode;
        while (p != null) {
            if (p.right == null) {
                p = p.down;
                continue;
            }
            int compare = comparator.compare(p.right.value, target);
            if (compare == 0) {
                p.right = p.right.right;
                p = p.down;
            } else if (compare > 0) {
                p = p.down;
            } else {
                p = p.right;
            }
        }
    }

    public void add(T target) {
        SkipNode<T> findNode = search(target);
        if(findNode != null) return;

        Stack<SkipNode<T>> downStack = new Stack<>();
        SkipNode<T> p = headNode;
        while (p != null) {
            if(p.right == null) {
                downStack.add(p);
                p = p.down;
            }else if(comparator.compare(p.right.value, target) > 0) {
                downStack.add(p);
                p = p.down;
            }else {
                p = p.right;
            }
        }
        SkipNode<T> downNode = null;
        int level = 1;
        while (!downStack.isEmpty()) {
            p = downStack.pop();

            SkipNode<T> newNode = new SkipNode<>(target, p.right, downNode);
            p.right = newNode;
            downNode = newNode;

            if(!isAddIndex(level)) { //返回false，不添加索引，直接返回
                break;
            }
            level++;
            if(level > this.maxLevel && level <= LEVEL_LIMIT) { //新添加一层
                this.maxLevel = level;
                SkipNode<T> highHeadNode = new SkipNode<>(null, null, null);
                highHeadNode.down = headNode;
                headNode = highHeadNode;
                downStack.add(headNode);
            }
        }
    }

    public List<T> getAll() {
        SkipNode<T> p = headNode;
        while (p.down != null) p = p.down;

        List<T> res = new ArrayList<>();

        while (p.right != null) {
            p = p.right;
            res.add(p.value);
        }
        return res;
    }

    private SkipNode<T> findBigger(T target, boolean isContain) {
        SkipNode<T> p = headNode;
        while (p != null) {
            int compare = comparator.compare(p.value, target);
            if(p.down == null) {//数据节点
                if(compare == 0) {
                    return isContain?p:p.right; //当前节点等于目标
                } else if(compare > 0) {//当前节点大于目标
                    return p; //返回节点
                } else{
                    p = p.right;   //当前节点小于目标，向右
                }
            }else { //索引节点
                if(compare == 0) { //等于，直接将p转到最底层的数据节点,等待下一个循环返回
                    while (p.down != null) p = p.down;
                }else if(compare < 0) { //小于，找到当前层下一个索引
                    if(p.right == null) //没有下一个，向下
                        p = p.down;
                    else if(comparator.compare(p.right.value, target) <= 0) {//下一个小于等于，向右
                        p = p.right;
                    }else {//下一个大于,向下
                        p = p.down;
                    }
                }
            }
        }
        return null;
    }
    private SkipNode<T> findLower(T target, boolean isContain) {
        SkipNode<T> findNode = null;
        if(isContain && (findNode = search(target)) != null) {
            return findNode;
        }

        Stack<SkipNode<T>> downStack = new Stack<>();
        SkipNode<T> p = headNode;
        while (p != null) {
            if(p.right == null) {
                downStack.add(p);
                p = p.down;
            }else if(comparator.compare(p.right.value, target) >= 0) {
                downStack.add(p);
                p = p.down;
            }else {
                p = p.right;
            }
        }
        return downStack.pop();
    }
    public List<T> search(T min, T max, boolean isContain) {
        if(comparator.compare(min, max) >= 0) {
            return Collections.emptyList();
        }

        SkipNode<T> l = findBigger(min, isContain);
        SkipNode<T> r = findLower(max, isContain);
        if(l == null) { //没有比min大的，
            return Collections.emptyList();
        }
        if(r == null) { //没有比max小
            return Collections.emptyList();
        }
        List<T> res = new ArrayList<>();
        while (!l.equals(r)){
            res.add(l.value);
            l = l.right;
        }
        res.add(r.value);
        return res;
    }
    private static boolean isAddIndex(int n) {
        double probability = 1.0 / Math.pow(2, n - 1);
        return Math.random() < probability;
    }

    public static void main(String[] args) {
        SkipList<Integer> skipList = new SkipList<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        });

        List<Integer> list = new ArrayList<>();
        final Random random = new Random();
        for (int i = 0; i < 200; i++) {
            int next = random.nextInt(100000);
            skipList.add(next);
            list.add(next);
        }
        list.sort(Integer::compareTo);

        List<Integer> all = skipList.getAll();

        System.out.println(all.size());
        System.out.println(list.size());
        System.out.println(list);
        System.out.println(all);
    }
}
class SkipNode<T> {
    protected T value;
    protected SkipNode<T> right;
    protected SkipNode<T> down;

    public SkipNode(T value, SkipNode<T> right, SkipNode<T> down) {
        this.value = value;
        this.right = right;
        this.down = down;
    }

    @Override
    public String toString() {
        return "SkipNode{" +
                "value=" + value +
                '}';
    }
}
