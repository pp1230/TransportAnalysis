import java.io.Serializable;

public class Group implements Serializable {
    private String center;
    private String city;
    public Group(String center, String city){
        this.center = center;
        this.city = city;
    }

    public String getCenter() {
        return center;
    }

    public void setCenter(String center) {
        this.center = center;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
