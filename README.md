# android-db

简单的orm实现

```java
//创建表
@Table(name = "test")
public class Test {
    @AutoIncrement
    @PrimaryKey
    @Column(name= "id")
    private long id;
    @Column(name = "name")
    private String name;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

Client dbClient = new SqlCipherClient(db);
dbClient.createTable(Test.class);

//查询
dbClient.get(Test.class);
//视图查询

@TableView(query = "select name from test")
public class Test {
    @Column(name = "name")
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
```
