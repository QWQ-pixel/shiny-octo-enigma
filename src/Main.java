import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class Main { // имена файлов не содержит пути и слеша
    public static void main(String[] args) {
        try {

            Configuration conf = new Configuration (); // создаем экземпляр объекта конфигурации
            URI uri = new URI("hdfs://localhost:9000");
            FileSystem fs = FileSystem.get (uri, conf, "NIto"); // Получить объект файловой системы, три параметра соответственно привязаны к uri, conf, текущей учетной записи пользователя
           // if("put"){uploadFile(fs);} может лучше через switch
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void uploadFile(FileSystem fileSystem, String filePath){
        try{
            String dstDir = "/user/NIto"; // каталог пользователя hdfs
            Path src = new Path (filePath); // Привязать путь пути
            Path dst = new Path(dstDir);

            /*загрузить файлы*/
            fileSystem.copyFromLocalFile (src, dst); // Вызов команды copyFromLocal
            fileSystem.close (); // Закрываем объект файловой системы
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("File uploaded!");
    }
    public void deleteFile(String fileName){
        //передаем файл в ф ю проверяем существует ли он удаляем

    }
    public void getFile(String fileName, FileSystem fileSystem, String localPath){ //пересмотреть ф ю
        //проверяем на наличие выводим если есть если нет выводим что нет
        FSDataInputStream in = null;
        try {
            in = fileSystem.open(new Path(fileName));
            OutputStream out = new FileOutputStream(localPath); //куда будем скачивать скорее всего предустановим директорию
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(in != null){
                IOUtils.closeStream(in);
            }
        }
    }
    public void createDir(String nameDir, FileSystem fileSystem) throws IOException {
        Path path = new Path(nameDir);
        if (fileSystem.exists(path)) {
            return;
        }
        fileSystem.mkdirs(path);
    }
    public void append(String localFile, String hdfsFile){

    }
}
