import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import java.util.Scanner;

public class Main { // имена файлов не содержит пути и слеша
    //− mkdir <имя каталога в HDFS> (создание каталога в HDFS);
    //− put <имя локального файла> (загрузка файла в HDFS);
    //− get <имя файла в HDFS> (скачивание файла из HDFS);
    //− append <имя локального файла> <имя файла в HDFS> (конкатенация
    //файла в HDFS с локальным файлом);
    //− delete <имя файла в HDFS> (удаление файла в HDFS);
    //− ls (отображение содержимого текущего каталога в HDFS с разделением
    //файлов и каталогов);
    //− cd <имя каталога в HDFS> (переход в другой каталог в HDFS, ".." — на
    //уровень выше);
    //− lls (отображение содержимого текущего локального каталога с
    //разделением файлов и каталогов);
    //− lcd <имя локального каталога> (переход в другой локальный каталог,
    //".." — на уровень выше).
    public static void main(String[] args) {
        try {

            Configuration conf = new Configuration (); // создаем экземпляр объекта конфигурации
            URI uri = new URI("hdfs://localhost:9000");
            FileSystem fs = FileSystem.get (uri, conf, "NIto"); // Получить объект файловой системы, три параметра соответственно привязаны к uri, conf, текущей учетной записи пользователя
           // if("put"){uploadFile(fs);} может лучше через switch
            Scanner keyboard = new Scanner(System.in);
            //System.out.println("enter an integer");
            //String path = keyboard.nextLine();
            //list(new Path("/"), fs);
            getLDInfo();
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
    public static void list(Path path, FileSystem fileSystem) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        for(FileStatus file:fileStatuses){
            if(file.isFile()){
                System.out.println(file.getPath().toString()); // сюда файл
            }else{ //сюда каталог
                String nameDir = file.getPath().toString();
                String[] parse = nameDir.split("/");
                nameDir = parse[parse.length-1];
                System.out.println(nameDir);
            }
        }
    }
    public static void getLDInfo(){
        File dir = new File(System.getProperty("user.dir"));
        File [] files = dir.listFiles();
        for(File item : files){
            if(item.isDirectory()){
                System.out.println(item.getName() + "\t folder");
            }
            else{
                System.out.println(item.getName() + "\t file");
            }
        }
    }
}
