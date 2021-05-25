import java.io.*; 
import java.net.*; 
import java.util.Scanner; 
  
public class Client  
{ 
    public static void main(String[] args) throws IOException  
    { 
        try
        { 
            Scanner scn = new Scanner(System.in); 
            System.out.println("Ingrese la IP del servidor a conectarse"); // Se pide el ingreso de la IP del servidor sin "" (Ex: 127.0.0.1)
            String ip = scn.nextLine();
            System.out.println("Peticion enviada al servidor");
            Socket s = new Socket(ip, 6868);   // Se crea el socket con el puerto 6868 por defecto.
            DataInputStream dis = new DataInputStream(s.getInputStream()); 
            DataOutputStream dos = new DataOutputStream(s.getOutputStream());
            String entrada = "1";
            while (true)  
            {
                if(entrada.equals("1")){
                    System.out.println("Conexion aceptada por el servidor");  
                }
                System.out.println("Que desea hacer (ls - put [archivo]- get [archivo]- delete [archivo] - exit)"); 
                String tosend = scn.nextLine(); // 
                dos.writeUTF(tosend); // Se manda el string que se acaba de leer al servidor (put,ls,get,delete)
                String dividir2 []; // Se crea un string para dividir el string que se mando en "tosend"
                dividir2 = tosend.split(" "); // Se divide en dos, para poder ver el archivo y el comando en cada caso.
                if(tosend.equals("exit"))  // En caso de escribir exit, se termina la conexion del cliente.
                {  
                    s.close(); 
                    System.out.println("conexion cerrada"); 
                    break; 
                } 
                else if(dividir2[0].equals("ls")){ // En el caso de ls se lee lo respondido por el servidor y ahora pasa al print de Que desea hacer (siguiente comando)               
                String received = dis.readUTF(); 
                System.out.println(received);                    
                }
                else if(dividir2[0].equals("delete")){ // En este caso de Delete se lee lo del servidor y ahora pasa al print de Que desea hacer (Siguiente comando)
                    String received = dis.readUTF(); 
                    System.out.println(received); 
                    }
                else if(dividir2[0].equals("put")){  // Caso PUT
                    try{
                    File myFile = new File(dividir2[1]);  // Se crea un File con el archivo que se escribio
                    byte[] mybytearray = new byte[(int) myFile.length()]; // Bye que contiene el length del archivo

                    FileInputStream fis2 = new FileInputStream(myFile); 
                    BufferedInputStream bis2 = new BufferedInputStream(fis2);
                    DataInputStream dis2 = new DataInputStream(bis2);
                    dis2.readFully(mybytearray, 0, mybytearray.length);
                    OutputStream os2 = s.getOutputStream();
                    DataOutputStream dos2 = new DataOutputStream(os2);
                    dos2.writeUTF(myFile.getName()); // Se manda al servidor el nombre del archivo
                    dos2.writeLong(mybytearray.length); // Se manda el length del byte
                    dos2.write(mybytearray, 0, mybytearray.length); 
                    dos2.flush(); //
                    System.out.println("Enviado a servidor");  
                }   
                catch (Exception e) {
                        System.err.println("ERROR! " + e);
                    }
                }
                else if(dividir2[0].equals("get")){ // Caso GET
                    int bytesRead;
                    InputStream in = s.getInputStream();
                    DataInputStream clientData = new DataInputStream(in);
                    String fileName = clientData.readUTF(); // Se lee el archivo a recibir
                    OutputStream output = new FileOutputStream((fileName)); // Archivo a recibir
                    long size = clientData.readLong();
                    byte[] buffer = new byte[1024];
                    while (size > 0   // En este while mientras hayan bytes por leer se lee el archivo enviado desde el servidor y escribe en el
                            && (bytesRead = clientData.read(buffer, 0,
                                    (int) Math.min(buffer.length, size))) != -1) {
                        output.write(buffer, 0, bytesRead);
                        size -= bytesRead;
                    }
                    output.flush();
                    System.out.println(fileName + " recibido desde servidor");
                }
                else{
                String received = dis.readUTF(); 
                System.out.println(received);
                }
            entrada="2";
            } 
            scn.close();  // Se cierran los Streams
            dis.close(); // Se cierran los Streams
            dos.close(); // Se cierran los Streams
        }catch(Exception e){ 
            e.printStackTrace(); 
        } 
    } 
} 