--DROP FUNCTION fenamosq.change_passwd(character varying, character varying, character varying, character varying, timestamp without time zone, numeric, character varying);

CREATE OR REPLACE FUNCTION fenamosq.change_passwd(
    IN pi_old_password character varying,
    IN pi_new_password character varying,
    IN pi_new_password_2 character varying,
    IN pi_master character varying,
    IN pi_tran_date timestamp without time zone,
    IN pi_tran_id numeric,
    IN pi_tran_user character varying,
    OUT po_tran_id numeric,
    OUT po_cod_err character varying,
    OUT po_msj_err character varying,
    OUT po_msj_err_pl character varying)
  RETURNS record AS
$BODY$
  DECLARE
      c_cod_transaction  varchar(200):='START_SESSION_SYS';
      v_tran_id          NUMERIC;
      v_tran_user        VARCHAR(50);
      v_tran_date        TIMESTAMP;      
      v_validar          varchar(20);
      v_pass_in    varchar(2000);
      v_pass_register  varchar(2000);
      c_falso            varchar(20):='0';
      v_aplica           varchar(2):='1';
      c_aplica           varchar(2):='1';
      v_old_password   varchar(2000);
  BEGIN 
       
      --Obtener valores transacccionales
      IF pi_master = c_aplica THEN 
          
          SELECT rs.po_tran_id,
                 rs.po_tran_date,
                 rs.po_cod_err,
                 rs.po_msj_err,
                 rs.po_msj_err_pl
           INTO  v_tran_id,
                 v_tran_date,
                 po_cod_err,
                 po_msj_err,
                 po_msj_err_pl
           FROM fenamosq.get_transaction(
                pi_cod_transaction    => c_cod_transaction,
                pi_tran_user          => pi_tran_user
           ) rs;

          IF po_cod_err <> 'OK' THEN 
              RAISE EXCEPTION 'TRANSFER';
          END IF;

      ELSIF pi_tran_date IS NULL OR  pi_tran_id IS NULL  OR pi_tran_user  IS NULL THEN
          po_cod_err := 'PASS-CHANGE-001';
          RAISE EXCEPTION 'CONDICTION';
      ELSE 
          v_tran_id := pi_tran_id;
          v_tran_date  := pi_tran_date;
      END IF; 

      SELECT rs.po_aplica 
        INTO v_aplica 
        FROM fenamosq.validate_transaction(
              pi_cod_transaction => c_cod_transaction,
              pi_tran_user        => v_tran_user
            ) rs;

      IF v_aplica = '0' THEN 
          po_cod_err := 'PASS-CHANGE-002';
          RAISE EXCEPTION 'CONDICTION';
      ELSIF v_aplica = '2' THEN 
          po_cod_err := 'PASS-CHANGE-003';
          RAISE EXCEPTION 'CONDICTION';
      END IF;


    po_cod_err := 'PASS-CHANGE-004';
    SELECT rs 
      INTO v_old_password
      FROM encrypt(pi_old_password::bytea,'1234'::bytea,'aes'::text) rs;

    po_cod_err := 'PASS-CHANGE-005';
    SELECT passwd
      INTO v_pass_register
      FROM fenamosq.emp_user
     WHERE cod_user = pi_tran_user;



     IF v_old_password <> v_pass_register THEN 
         po_cod_err := 'PASS-CHANGE-006';
         RAISE EXCEPTION 'CONDICTION';

     ELSIF pi_new_password <> pi_new_password_2 THEN

        po_cod_err := 'PASS-CHANGE-007';
        RAISE EXCEPTION 'CONDICTION';

     ELSE
        UPDATE fenamosq.emp_user
           SET passwd = encrypt(pi_new_password::bytea,'1234'::bytea,'aes'::text)
         WHERE cod_user = pi_tran_user;
     END IF;

     
     po_cod_err := 'OK'||pi_tran_user;
     po_tran_id := v_tran_id;
  EXCEPTION
    WHEN OTHERS THEN 
        po_tran_id := v_tran_id;

        IF(SQLERRM <> 'TRANSFER') THEN 
            po_msj_err_pl = SQLERRM;
        END IF;

        IF pi_master = c_aplica THEN
          
          SELECT rs.po_msj_err
            into po_msj_err 
            FROM fenamosq.set_transaction_log(
                pi_tran_id         => v_tran_id,
                pi_tran_cod        => c_cod_transaction,
                pi_tran_date       => v_tran_date,    
                pi_tran_user       => pi_tran_user,
                pi_cod_err         => po_cod_err,
                pi_msj_err_pl      => po_msj_err_pl) rs;
        END IF;


  END; 
  $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION fenamosq.change_passwd(character varying, character varying, character varying, character varying, timestamp without time zone, numeric, character varying)
  OWNER TO fenamo;
