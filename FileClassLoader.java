package map_reduce_demo;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class FileClassLoader extends ClassLoader{
	FileInputStream Dis;
	public FileClassLoader(FileInputStream dis){
		Dis = dis;
	}
	
	protected Class<?> findClass(String name) throws ClassNotFoundException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
    			final int buffersize = 1024;
    			byte[] buf = new byte[buffersize];
    			while(true){
    				int read = 0;
    				if( (read = Dis.read(buf) ) == -1 ){
    					break;
    				}
    			bos.write(buf,0,read);
    			}
            } catch (IOException e) {
                e.printStackTrace();
            }

            byte[] data = bos.toByteArray();
            Dis.close();
            bos.close();

            return defineClass(name,data,0,data.length);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return super.findClass(name);
    }
}
